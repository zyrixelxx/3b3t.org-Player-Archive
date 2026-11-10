#Zyxel hand scanner
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

BASE = "https://cf.3b3t.org"
OUT_FILE = "players_stats.json"
CACHE_FILE = "players_cache.jsonl"
CATEGORY = "playtime"    # category to browse leaderboard by
PAGE_SIZE = 50           # maximum per docs
WORKERS = 10             # threads; decrease if hitting 429
DEFAULT_DESC = "No description"

STAT_KEYS = [
    "playtime", "kills", "deaths", "blocksPlaced", "blocksMined",
    "obsidianPlaced", "enderchestsPlaced", "carpetsPlaced", "crystalsUsed",
    "totemsUsed", "expBottlesUsed", "advancementsCompleted",
]

lock = threading.Lock()


def get_json(path, params=None, retries=6):
    """GET with retries. 404 -> None."""
    for attempt in range(retries):
        try:
            r = requests.get(BASE + path, params=params, timeout=20)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(float(r.headers.get("Retry-After", 2 * (attempt + 1))))
                continue
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError):
            time.sleep(1.5 * (attempt + 1))
    return None


def collect_leaderboard():
    """Browse leaderboard until an empty page. Returns {username: stats}."""
    players = {}
    page = 0
    while True:
        data = get_json("/api/statistics/leaderboard",
                        {"category": CATEGORY, "page": page, "pageSize": PAGE_SIZE})
        entries = (data or {}).get("entries") or []
        if not entries:
            break
        new = 0
        for e in entries:
            p = e.get("player") or {}
            name = p.get("username")
            if name and name not in players:
                players[name] = p.get("stats") or {}
                new += 1
        print(f"leaderboard: page {page}, total usernames {len(players)}")
        if new == 0:  # protection against infinite loop if API returns the same data
            break
        page += 1
    return players


def build_record(username, lb_stats):
    player = (get_json("/api/statistics/player", {"username": username}) or {}).get("player") or {}
    stats_src = player.get("stats") or lb_stats or {}
    stats = {k: stats_src.get(k, 0) for k in STAT_KEYS}
    stats["regdate"] = player.get("regdate")

    card = get_json("/api/profile/public-card", {"username": username}) or {}
    bio = (card.get("bio") or "").strip()

    return {
        "nickname": username,
        "stats": stats,
        "description": bio if bio else DEFAULT_DESC,
    }


def load_cache():
    done = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done[rec["nickname"]] = rec
                except (ValueError, KeyError):
                    pass
    return done


def main():
    leaderboard = collect_leaderboard()
    done = load_cache()
    todo = [n for n in leaderboard if n not in done]
    print(f"total: {len(leaderboard)}, already in cache: {len(done)}, remaining: {len(todo)}")

    counter = {"n": 0}
    cache_f = open(CACHE_FILE, "a", encoding="utf-8")

    def task(name):
        rec = build_record(name, leaderboard[name])
        with lock:
            cache_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            cache_f.flush()
            done[name] = rec
            counter["n"] += 1
            if counter["n"] % 100 == 0:
                print(f"processed {counter['n']}/{len(todo)}")

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            list(ex.map(task, todo))
    finally:
        cache_f.close()

    players = [done[n] for n in leaderboard if n in done]
    result = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_players": len(players),
        "players": players,
    }
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"done: {len(players)} players -> {OUT_FILE}")


if __name__ == "__main__":
    main()
