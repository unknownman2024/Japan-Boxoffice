import requests
import time
import random
import uuid
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

#########################################
# CONFIG
#########################################

MOVIE_LIST_URL = "https://khaltimovies.text2024mail.workers.dev/"
MOVIE_INFO_URL = "https://khalti.com/api/v5/movie-info/{movie_id}"
SHOWINFO_URL   = "https://khalti.com/api/v2/service/use/movie/showinfo-v2/"
TOKEN_URL      = "https://boxoffice24.pages.dev/Nepal/khaltitoken.txt"

IST  = ZoneInfo("Asia/Kolkata")
FIXED_DATE = datetime(2026, 1, 9, tzinfo=IST)   # 🔒 Fixed base date
TODAY_IST  = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)

# 👉 If today is Jan 9 or later, move to next+2 day
if TODAY_IST >= FIXED_DATE:
    TRACK_DATE = TODAY_IST + timedelta(days=2)
else:
    TRACK_DATE = FIXED_DATE

DATE = TRACK_DATE.strftime("%Y-%m-%d")

OUT_DIR = "Nepal Advance"                                            # ✅ NEW FOLDER

MAX_WORKERS = 10
MAX_RETRIES = 5
TIMEOUT = 15
GLOBAL_COOLDOWN_SEC = 6

#########################################
# ATOMIC JSON WRITE
#########################################

def atomic_dump(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

#########################################
# LOGGER
#########################################

def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

#########################################
# TOKEN (FETCH ONLY ONCE)
#########################################

AUTH_TOKEN = None

def init_token_once():
    global AUTH_TOKEN
    log("🔑 Fetching token once...")
    r = requests.get(TOKEN_URL, timeout=TIMEOUT)
    r.raise_for_status()
    AUTH_TOKEN = r.text.strip()
    log("✅ Token locked for this run")

#########################################
# GLOBAL 429 COOLDOWN
#########################################

cooldown_lock = threading.Lock()
cooldown_until = 0
cooldown_active = False

def trigger_global_cooldown():
    global cooldown_until, cooldown_active
    with cooldown_lock:
        cooldown_until = time.time() + GLOBAL_COOLDOWN_SEC
        if not cooldown_active:
            log(f"🧊 GLOBAL COOLDOWN for {GLOBAL_COOLDOWN_SEC}s")
            cooldown_active = True

def wait_if_global_cooldown():
    global cooldown_active
    while True:
        with cooldown_lock:
            remaining = cooldown_until - time.time()
        if remaining <= 0:
            if cooldown_active:
                log("✅ Cooldown ended → resume")
                cooldown_active = False
            return
        time.sleep(min(1.0, remaining))

#########################################
# RANDOM HELPERS
#########################################

def random_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

def random_user_agent():
    chrome = random.randint(120, 135)
    android = random.randint(6, 11)
    model = random.choice(["Pixel 4", "Nexus 5", "Moto G5", "Galaxy S7", "Redmi Note 8"])
    return (
        f"Mozilla/5.0 (Linux; Android {android}; {model}) "
        f"AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{chrome}.0.0.0 Mobile Safari/537.36"
    )

def random_device_id():
    return "kwa-" + uuid.uuid4().hex[:16]

#########################################
# SAFE REQUEST
#########################################

def safe_request(method, url, **kwargs):
    retries = MAX_RETRIES

    while True:
        wait_if_global_cooldown()

        try:
            r = requests.request(method, url, timeout=TIMEOUT, **kwargs)

            if r.status_code == 429:
                trigger_global_cooldown()
                if retries <= 0:
                    raise Exception("HTTP 429")
                retries -= 1
                continue

            if not r.ok:
                if retries <= 0:
                    r.raise_for_status()
                time.sleep(1.5)
                retries -= 1
                continue

            return r

        except (requests.ConnectionError, requests.Timeout):
            if retries <= 0:
                raise
            time.sleep(1.5)
            retries -= 1

#########################################
# FETCH SINGLE SHOW SUMMARY
#########################################

def fetch_show_summary(movie_id, movie_name, show_id):
    try:
        headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": AUTH_TOKEN,
            "content-type": "application/json",
            "deviceid": random_device_id(),
            "origin": "https://web.khalti.com",
            "referer": "https://web.khalti.com",
            "user-agent": random_user_agent(),
            "x-forwarded-for": random_ip(),
        }

        payload = {"show_id": show_id, "new_layout": False}
        r = safe_request("POST", SHOWINFO_URL, headers=headers, json=payload)
        data = r.json()

        seat_data = data.get("new_seats") or []
        showinfo  = data.get("showinfo") or {}
        tickets   = showinfo.get("tickets") or []

        total = {"seats": 0, "sold": 0, "reserved": 0, "available": 0, "gross": 0}
        ticket_types = {}

        for row in seat_data:
            for s in row.get("seats", []):
                if not s.get("is_active") or s.get("seat_status") == "Gap":
                    continue

                t = s.get("ticket_type")
                ticket_types.setdefault(t, {"price": 0, "seats": 0, "sold": 0, "reserved": 0, "available": 0})

                tt = ticket_types[t]
                tt["seats"] += 1
                if s.get("seat_status") == "Sold":
                    tt["sold"] += 1
                elif s.get("seat_status") == "Reserved":
                    tt["reserved"] += 1
                else:
                    tt["available"] += 1

        for t in tickets:
            level = t.get("price_level")
            price = round((t.get("price") or 0) / 100)
            if level in ticket_types:
                ticket_types[level]["price"] = price

        for tt in ticket_types.values():
            gross = tt["price"] * (tt["sold"] + tt["reserved"])
            total["seats"] += tt["seats"]
            total["sold"] += tt["sold"]
            total["reserved"] += tt["reserved"]
            total["available"] += tt["available"]
            total["gross"] += gross

        occ = round(100 * (total["sold"] + total["reserved"]) / total["seats"], 2) if total["seats"] else 0

        show = showinfo.get("show", {})
        dt   = show.get("datetime") or ""
        date_part = dt.split(" ")[0]
        time_part = dt.split(" ")[1][:5] if " " in dt else ""

        theatre_full = f"{show.get('theatre_name')} - {show.get('auditorium_name')}"
        venue = show_id.split(":")[1] if ":" in show_id else show_id

        return {
            "movie_id": movie_id,
            "movie_name": movie_name,
            "show_id": show_id,
            "venue": venue,
            "theatre": theatre_full,
            "date": date_part,
            "time": time_part,
            "seats": total["seats"],
            "sold": total["sold"],
            "reserved": total["reserved"],
            "available": total["available"],
            "gross": total["gross"],
            "occupancy_percent": occ
        }

    except Exception as e:
        return {
            "movie_id": movie_id,
            "movie_name": movie_name,
            "show_id": show_id,
            "venue": None,
            "theatre": None,
            "date": DATE,
            "time": None,
            "seats": 0,
            "sold": 0,
            "reserved": 0,
            "available": 0,
            "gross": 0,
            "occupancy_percent": 0,
            "error": str(e),
            "skipped": True
        }

#########################################
# FETCH MOVIE LIST
#########################################

def fetch_movie_list():
    log("📥 Fetching movie list...")
    r = safe_request("GET", MOVIE_LIST_URL)
    data = r.json()
    movies = data.get("movies", [])
    log(f"🎬 Movies found: {len(movies)}")
    return [{"id": m.get("idx"), "name": m.get("name")} for m in movies]

#########################################
# PROCESS ONE MOVIE (NO CUTOFF)
#########################################

def process_single_movie(movie_id, movie_name):
    log(f"\n🎥 Processing: {movie_name}")

    r = safe_request("GET", MOVIE_INFO_URL.format(movie_id=movie_id))
    movie_json = r.json()

    theatres = movie_json.get("theatres", [])
    show_ids = []

    for t in theatres:
        for s in t.get("shows", []):
            show_date = (s.get("datetime") or "").split(" ")[0]
            if show_date == DATE:
                show_ids.append(s.get("show_id"))

    total_shows = len(show_ids)
    log(f"🎯 {movie_name} → {total_shows} advance shows found")

    if total_shows == 0:
        return []

    results = []
    completed = 0
    lock = threading.Lock()

    def wrapped(sid):
        nonlocal completed
        res = fetch_show_summary(movie_id, movie_name, sid)
        with lock:
            completed += 1
            log(f"📊 {movie_name} → {completed}/{total_shows}")
        return res

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        jobs = [pool.submit(wrapped, sid) for sid in show_ids]
        for j in as_completed(jobs):
            results.append(j.result())

    return results

#########################################
# SUMMARY BUILDER (MOVIE + VENUE WISE)
#########################################

def build_summary_by_movie(all_rows):
    movies = {}

    for s in all_rows:
        mid   = s["movie_id"]
        name  = s["movie_name"]
        venue = s.get("venue") or "Unknown"

        movies.setdefault(mid, {
            "movie_id": mid,
            "movie_name": name,
            "total_shows": 0,
            "seats": 0,
            "sold": 0,
            "reserved": 0,
            "available": 0,
            "housefull": 0,
            "fastfilling": 0,
            "venues": {}
        })

        m = movies[mid]

        m["total_shows"] += 1
        m["seats"] += s["seats"]
        m["sold"] += s["sold"]
        m["reserved"] += s["reserved"]
        m["available"] += s["available"]

        occ = s["occupancy_percent"]
        if occ >= 98:
            m["housefull"] += 1
        elif 50 <= occ < 98:
            m["fastfilling"] += 1

        m["venues"].setdefault(venue, {
            "venue": venue,
            "total_shows": 0,
            "seats": 0,
            "sold": 0,
            "reserved": 0,
            "available": 0,
            "housefull": 0,
            "fastfilling": 0
        })

        v = m["venues"][venue]
        v["total_shows"] += 1
        v["seats"] += s["seats"]
        v["sold"] += s["sold"]
        v["reserved"] += s["reserved"]
        v["available"] += s["available"]

        if occ >= 98:
            v["housefull"] += 1
        elif 50 <= occ < 98:
            v["fastfilling"] += 1

    final = []
    for mv in movies.values():
        mv["venues"] = list(mv["venues"].values())
        final.append(mv)

    return final

#########################################
# MAIN
#########################################

def main():
    print("\n🚀 Nepal Advance Boxoffice Tracker Started...\n")

    os.makedirs(OUT_DIR, exist_ok=True)

    summary_file = f"{OUT_DIR}/{DATE}_Summary.json"
    detail_file  = f"{OUT_DIR}/{DATE}_Detailed.json"

    existing_rows = []
    if os.path.exists(detail_file):
        try:
            existing_rows = json.load(open(detail_file)).get("shows", [])
            log(f"📁 Loaded old DB: {len(existing_rows)} shows")
        except:
            log("⚠ Old DB corrupted, starting fresh")

    init_token_once()
    movie_list = fetch_movie_list()

    new_rows = []
    for m in movie_list:
        new_rows.extend(process_single_movie(m["id"], m["name"]))

    log(f"\n🆕 Newly fetched this run: {len(new_rows)}")

    # ✅ MERGE (NEVER DELETE)
    data_map = {s["show_id"]: s for s in existing_rows}
    for s in new_rows:
        data_map[s["show_id"]] = s

    all_rows = list(data_map.values())

    movies_summary = build_summary_by_movie(all_rows)
    timestamp = datetime.now(IST).strftime("%I:%M %p, %d %B %Y")

    atomic_dump(detail_file, {"date": DATE, "lastUpdated": timestamp, "shows": all_rows})
    atomic_dump(summary_file, movies_summary)

    print("\n================================================")
    print(f"🎬 Movies Covered: {len(movies_summary)}")
    print(f"🎟 Lifetime Shows Stored: {len(all_rows)}")
    print(f"📁 Summary  → {summary_file}")
    print(f"📁 Detailed → {detail_file}")
    print("================================================")
    print("🎉 DONE — NEPAL ADVANCE MODE ACTIVE\n")

if __name__ == "__main__":
    main()
