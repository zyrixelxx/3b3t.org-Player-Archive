#!/usr/bin/env python3
"""
stalker99699 stats scanner (Linux / systemd версия).

Работает как долгоживущий демон:
- раз в час проверяет, собран ли текущий месяц;
- 1-го числа (или при первом запуске в новом месяце) собирает статистику
  по ВСЕМ категориям и объединяет ники в один максимальный список;
- кэш категорий позволяет продолжить работу после аварийного отключения;
- при HTTP 429 автоматически понижает количество воркеров;
- по завершении запускает внешний script.sh.

Раскладка:
  <dir>/scan_stats/YYYY/MM/players_stats.jsonl        - итоговые данные
  <dir>/cache/YYYY/MM/category_<category>.jsonl       - кэш лидерборда категории
  <dir>/statick.txt                                    - счётчик месяцев

Подробное описание структуры записей см. в help.txt рядом со скриптом.
"""

import json
import os
import signal
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

# ==== Конфигурация ====
BASE = "https://cf.3b3t.org"
PAGE_SIZE = 50             # максимум, который принимает API (больше -> отдаёт 5)
WORKERS_INITIAL = 10
WORKERS_MIN = 1
DEFAULT_DESC = "Нет описания"

CHECK_INTERVAL = 3600      # 1 час между проверками
RETRY_INTERVAL = 1800      # 30 мин между повторами после сбоя

STAT_KEYS = [
    "playtime", "kills", "deaths", "blocksPlaced", "blocksMined",
    "obsidianPlaced", "enderchestsPlaced", "carpetsPlaced", "crystalsUsed",
    "totemsUsed", "expBottlesUsed", "advancementsCompleted",
]

# Собираем объединение по всем категориям, чтобы получить максимальный список ников.
CATEGORIES = list(STAT_KEYS)

# Скрипт, который нужно запускать после успешной сборки.
# Путь можно переопределить через env-переменную ZYRIXEL_POST_SCRIPT.
POST_SCRIPT = os.environ.get(
    "ZYRIXEL_POST_SCRIPT",
    "/opt/zyrixel/script.sh",
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATICK_FILE = os.path.join(SCRIPT_DIR, "statick.txt")

# ==== Runtime state ====
stop_event = threading.Event()

# Динамический лимит параллельных воркеров (уменьшается на 429, плавно растёт)
_conc = {"limit": WORKERS_INITIAL, "active": 0, "successes": 0}
_conc_cond = threading.Condition()


# ==== Логирование ====
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


# ==== Обработка systemd-сигналов ====
def handle_signal(signum, _frame):
    log(f"получен сигнал {signum}, завершаемся")
    stop_event.set()
    with _conc_cond:
        _conc_cond.notify_all()


# ==== Управление воркерами ====
def acquire_slot():
    with _conc_cond:
        while _conc["active"] >= _conc["limit"]:
            if stop_event.is_set():
                return False
            _conc_cond.wait(timeout=1.0)
        _conc["active"] += 1
        return True


def release_slot():
    with _conc_cond:
        _conc["active"] -= 1
        _conc_cond.notify()


def reduce_workers():
    with _conc_cond:
        old = _conc["limit"]
        _conc["limit"] = max(WORKERS_MIN, old // 2)
        _conc["successes"] = 0
        if _conc["limit"] != old:
            log(f"HTTP 429: воркеров {old} -> {_conc['limit']}")
            _conc_cond.notify_all()


def note_success():
    """После серии успешных запросов плавно возвращаем воркеров."""
    with _conc_cond:
        if _conc["limit"] >= WORKERS_INITIAL:
            return
        _conc["successes"] += 1
        if _conc["successes"] >= 25:
            _conc["successes"] = 0
            _conc["limit"] += 1
            log(f"восстановление: воркеров -> {_conc['limit']}")
            _conc_cond.notify_all()


def reset_workers():
    with _conc_cond:
        _conc["limit"] = WORKERS_INITIAL
        _conc["successes"] = 0
        _conc_cond.notify_all()


# ==== HTTP ====
def get_json(path, params=None, retries=6):
    for attempt in range(retries):
        if stop_event.is_set():
            return None
        try:
            r = requests.get(BASE + path, params=params, timeout=20)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                reduce_workers()
                wait = float(r.headers.get("Retry-After", 2 * (attempt + 1)))
                log(f"429 на {path}, ждём {wait:.0f}s")
                if stop_event.wait(wait):
                    return None
                continue
            r.raise_for_status()
            note_success()
            return r.json()
        except (requests.RequestException, ValueError):
            if stop_event.wait(1.5 * (attempt + 1)):
                return None
    return None


# ==== Пути и файлы ====
def current_period():
    now = datetime.now()
    return now.year, now.month


def period_paths(year, month):
    mm = f"{month:02d}"
    stats_dir = os.path.join(SCRIPT_DIR, "scan_stats", str(year), mm)
    cache_dir = os.path.join(SCRIPT_DIR, "cache", str(year), mm)
    os.makedirs(stats_dir, exist_ok=True)
    os.makedirs(cache_dir, exist_ok=True)
    return {
        "stats_file": os.path.join(stats_dir, "players_stats.jsonl"),
        "cache_dir": cache_dir,
    }


def category_cache_path(paths, category):
    return os.path.join(paths["cache_dir"], f"category_{category}.jsonl")


def is_collected(year, month):
    return os.path.exists(period_paths(year, month)["stats_file"])


def load_category_cache(path):
    """Возвращает dict ников или None, если кэша нет/он повреждён."""
    if not os.path.exists(path):
        return None
    recs = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                name = rec.get("nickname")
                if name:
                    recs[name] = rec
    except (ValueError, OSError):
        return None
    return recs


def write_category_cache(path, records):
    """Атомарная запись кэша категории через tmp + os.replace."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_stats_file(stats_file, players):
    """Атомарная запись итогового файла через tmp + os.replace."""
    tmp = stats_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for rec in players:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, stats_file)


def append_statick(players_count):
    count = 1
    if os.path.exists(STATICK_FILE):
        with open(STATICK_FILE, encoding="utf-8") as f:
            count = sum(1 for l in f if l.strip()) + 1
    date_str = datetime.now().strftime("%Y-%m-%d")
    line = f"{count} {date_str} players={players_count}\n"
    with open(STATICK_FILE, "a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())
    log(f"statick: {line.strip()}")


# ==== Сбор данных ====
def entry_to_record(entry):
    """Преобразует запись лидерборда в промежуточную запись игрока."""
    p = entry.get("player") or {}
    name = p.get("username")
    if not name:
        return None
    src = p.get("stats") or {}
    stats = {k: src.get(k, 0) for k in STAT_KEYS}
    return {"nickname": name, "stats": stats, "regdate": p.get("regdate")}


def merge_stats(dst, src):
    """Объединяет статистику, беря максимум по каждому полю."""
    for k in STAT_KEYS:
        v = src.get(k) or 0
        if v > (dst.get(k) or 0):
            dst[k] = v


def fetch_category(category, cache_path):
    """Возвращает dict ников категории (с кэшем) или None при сбое."""
    cached = load_category_cache(cache_path)
    if cached is not None:
        log(f"{category}: из кэша {len(cached)} игроков")
        return cached

    first = get_json("/api/statistics/leaderboard",
                     {"category": category, "page": 0, "pageSize": PAGE_SIZE})
    if first is None:
        log(f"{category}: не удалось получить страницу 0")
        return None

    total = first.get("totalPlayers") or 0
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    entries = list(first.get("entries") or [])

    page_data = {}
    failures = []
    failures_lock = threading.Lock()

    def task(page):
        if stop_event.is_set():
            return
        if not acquire_slot():
            return
        try:
            data = get_json("/api/statistics/leaderboard",
                            {"category": category, "page": page, "pageSize": PAGE_SIZE})
            if data is None:
                with failures_lock:
                    failures.append(page)
            else:
                page_data[page] = data.get("entries") or []
        finally:
            release_slot()

    if pages > 1:
        with ThreadPoolExecutor(max_workers=WORKERS_INITIAL) as ex:
            futures = [ex.submit(task, p) for p in range(1, pages)]
            for _ in as_completed(futures):
                pass

    # Повторяем страницы, которые не удалось загрузить (до двух проходов).
    for _ in range(2):
        if stop_event.is_set() or not failures:
            break
        with failures_lock:
            retry = sorted(set(failures))
            failures.clear()
        log(f"{category}: повтор {len(retry)} страниц")
        for p in retry:
            task(p)

    if stop_event.is_set():
        return None

    with failures_lock:
        failed_left = len(set(failures))
    if failed_left:
        log(f"{category}: предупреждение, не загружено страниц: {failed_left}")

    for p in sorted(page_data):
        entries.extend(page_data[p])

    recs = {}
    for e in entries:
        rec = entry_to_record(e)
        if not rec:
            continue
        name = rec["nickname"]
        if name in recs:
            merge_stats(recs[name]["stats"], rec["stats"])
        else:
            recs[name] = rec

    write_category_cache(cache_path, list(recs.values()))
    log(f"{category}: собрано {len(recs)} игроков (страниц {pages})")
    return recs


def collect_all_players(paths):
    """Объединяет ники всех категорий в один dict. None при остановке."""
    players = {}
    for category in CATEGORIES:
        if stop_event.is_set():
            return None
        recs = fetch_category(category, category_cache_path(paths, category))
        if recs is None:
            if stop_event.is_set():
                return None
            log(f"{category}: пропущено из-за ошибки")
            continue
        for name, rec in recs.items():
            if name in players:
                merge_stats(players[name]["stats"], rec["stats"])
                if players[name].get("regdate") is None and rec.get("regdate"):
                    players[name]["regdate"] = rec["regdate"]
            else:
                players[name] = {
                    "stats": dict(rec["stats"]),
                    "regdate": rec.get("regdate"),
                }
        log(f"объединение: {len(players)} уникальных ников (после {category})")
    return players


def build_record(username, data):
    stats = {k: (data.get("stats") or {}).get(k, 0) for k in STAT_KEYS}
    stats["regdate"] = data.get("regdate")
    return {
        "nickname": username,
        "stats": stats,
        "description": DEFAULT_DESC,
    }


def run_post_script():
    if not POST_SCRIPT or not os.path.exists(POST_SCRIPT):
        log(f"post-script не найден: {POST_SCRIPT}")
        return
    log(f"запуск post-script: {POST_SCRIPT}")
    try:
        res = subprocess.run([POST_SCRIPT], check=False, timeout=3600)
        log(f"post-script завершён с кодом {res.returncode}")
    except Exception as e:
        log(f"ошибка post-script: {e}")


def run_collection(year, month):
    paths = period_paths(year, month)
    log(f"=== сбор за {year}-{month:02d} начат ===")
    reset_workers()

    players = collect_all_players(paths)
    if players is None:
        if stop_event.is_set():
            log("остановлено во время сбора, кэш категорий сохранён")
        else:
            log("сбор не удался, повтор позже")
        return False
    if not players:
        log("ники не собраны, повтор позже")
        return False

    log(f"итого уникальных ников: {len(players)}")
    records = [build_record(name, data) for name, data in players.items()]

    log(f"пишем {len(records)} записей в {paths['stats_file']}")
    write_stats_file(paths["stats_file"], records)

    append_statick(len(records))
    run_post_script()

    log(f"=== сбор за {year}-{month:02d} завершён ===")
    return True


# ==== Главный цикл демона ====
def main():
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    log("Zyrixel scanner запущен (owner: stalker99699)")
    log(f"рабочая директория: {SCRIPT_DIR}")
    log(f"post-script: {POST_SCRIPT}")
    log(f"воркеров при старте: {WORKERS_INITIAL}")
    log(f"категорий: {len(CATEGORIES)}")

    last_state = None
    while not stop_event.is_set():
        year, month = current_period()
        collected = is_collected(year, month)

        if collected:
            if last_state != "collected":
                log(f"период {year}-{month:02d} уже собран, спим")
                last_state = "collected"
            stop_event.wait(CHECK_INTERVAL)
            continue

        log(f"период {year}-{month:02d} не собран — запускаем сбор")
        last_state = "pending"
        ok = False
        try:
            ok = run_collection(year, month)
        except Exception as e:
            log(f"непредвиденная ошибка сбора: {e}")
        if stop_event.is_set():
            break
        if not ok:
            log(f"сбор не удался, повтор через {RETRY_INTERVAL}s")
            stop_event.wait(RETRY_INTERVAL)

    log("демон остановлен")


if __name__ == "__main__":
    main()
