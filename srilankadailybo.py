import cloudscraper
import random
import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed


#########################################
# CONFIG
#########################################
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 5
TIMEOUT_SEC = 1
CUT_OFF_MINUTES = 200  # Show valid if minsLeft <= this

OUT_DIR = "Sri Lanka Boxoffice"
IST = ZoneInfo("Asia/Kolkata")


#########################################
# RANDOM HEADERS (Fingerprint Rotation)
#########################################
def random_user_agent():
    ios = f"Mozilla/5.0 (iPhone; CPU iPhone OS {random.randint(15,18)}_{random.randint(0,7)} like Mac OS X) Version/{random.randint(16,18)}.0 Mobile Safari/604.1"
    android = f"Mozilla/5.0 (Linux; Android {random.choice(['10','11','12','13','14'])}) Chrome/{random.randint(110,125)} Mobile Safari/537.36"
    windows = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/{random.randint(110,125)} Safari/537.36"
    return random.choice([ios, android, windows])


def random_headers(is_json=False):
    h = {
        "User-Agent": random_user_agent(),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://lk.bookmyshow.com/",
        "Connection": "keep-alive"
    }
    if is_json:
        h["Content-Type"] = "application/json;charset=UTF-8"
    return h


#########################################
# SCRAPER SESSION
#########################################
scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})


def safe_request(url, method="GET", payload=None):
    last_err = "UNKNOWN"
    for _ in range(RETRY_PER_REQUEST):
        try:
            headers = random_headers(method == "POST")
            if method == "POST":
                r = scraper.post(url, json=payload, headers=headers, timeout=TIMEOUT_SEC)
            else:
                r = scraper.get(url, headers=headers, timeout=TIMEOUT_SEC)

            if r.status_code == 200:
                return r.json(), None

            last_err = f"HTTP_{r.status_code}"
        except Exception as e:
            last_err = str(e)

    return None, last_err


#########################################
# API CALLS
#########################################
def get_movies():
    url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
    body = {"regionCode": "SNLK", "page": 1, "limit": 200, "filters": {}}
    return safe_request(url, "POST", body)


def get_showtimes(event_code, date):
    url = f"https://lk.bookmyshow.com/pwa/api/de/showtimes/byevent?regionCode=SNLK&eventCode={event_code}&dateCode={date}"
    return safe_request(url)


#########################################
# PARSING HELPERS
#########################################
def extract_movies(raw):
    if not isinstance(raw, dict):
        return []
    if "nowShowing" in raw and "arrEvents" in raw["nowShowing"]:
        return raw["nowShowing"]["arrEvents"]
    if "arrEvents" in raw:
        return raw["arrEvents"]
    if "movies" in raw:
        return raw["movies"]
    return []


def extract_venues(raw, date):
    details = raw.get("BookMyShow", {}).get("ShowDetails", [])
    for d in details:
        if str(d.get("Date")) == str(date):
            return d.get("Venues", [])
    return []


#########################################
# PROCESS SINGLE SHOW
#########################################
def flatten(movie, venue, sh, date):
    session_id = sh.get("SessionId") or sh.get("ShowInstanceId") or sh.get("Id") or ""

    total = sum(int(c.get("MaxSeats", 0)) for c in sh.get("Categories", []))
    avail = sum(int(c.get("SeatsAvail", 0)) for c in sh.get("Categories", []))
    price = float(sh.get("MinPrice", 0))

    sold = total - avail
    gross = sold * price
    occupancy = round((sold / total * 100), 2) if total else 0

    bad = False
    if sold < 0 or gross < 0 or avail > total or total == 0:
        sold = 0
        gross = 0
        occupancy = 0
        bad = True

    return {
        "movie": movie,
        "venue": venue.get("VenueName"),
        "sessionId": str(session_id),
        "time": sh.get("ShowTime"),
        "totalSeats": total,
        "available": avail,
        "sold": sold,
        "gross": gross,
        "occupancy": occupancy,
        "date": date,
        "badData": bad
    }


#########################################
# SCRAPE ONE MOVIE
#########################################
def scrape_movie(movie, date, attempt):
    title = movie["EventTitle"]
    code = movie["ChildEvents"][movie["DefaultChildIndex"]]["EventCode"]

    res, err = get_showtimes(code, date)
    if not res:
        print(f"❌ Attempt {attempt} → {title} | {err}")
        return title, [], False

    venues = extract_venues(res, date)
    if not venues:
        print(f"⚠️ Attempt {attempt} → {title} | No shows")
        return title, [], False

    rows = []
    for v in venues:
        for sh in v.get("ShowTimes", []):
            rows.append(flatten(title, v, sh, date))

    print(f"✅ {title} → {len(rows)} shows")
    return title, rows, True


#########################################
# MAIN EXECUTION
#########################################
print("\n🚀 Sri Lanka Showtime Scraper Started...\n")

target_date = datetime.now(IST).strftime("%Y%m%d")

movies_raw, _ = get_movies()
movies = [m for m in extract_movies(movies_raw) if "EventTitle" in m]

print(f"🎞 Movies Found: {len(movies)}\n")

all_rows = []
pending = movies.copy()


#########################################
# RETRY SYSTEM
#########################################
for attempt in range(1, SCRAPE_PASSES + 1):
    if not pending:
        break

    print(f"\n🔁 PASS {attempt}/{SCRAPE_PASSES} — retrying {len(pending)} movies…")

    next_list = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        tasks = {pool.submit(scrape_movie, m, target_date, attempt): m for m in pending}

        for job in as_completed(tasks):
            _, rows, ok = job.result()
            if ok:
                all_rows.extend(rows)
            else:
                next_list.append(tasks[job])

    pending = next_list


#########################################
# REMOVE DUPLICATES (session-based)
#########################################
seen_keys = set()
cleaned = []

for s in all_rows:
    key = (s["movie"], s["venue"], s["sessionId"])
    if key not in seen_keys:
        seen_keys.add(key)
        cleaned.append(s)

all_rows = cleaned
print(f"📌 Unique Shows (session-linked): {len(all_rows)}")


#########################################
# TIME FILTER (Cut-off)
#########################################
def parse_time(date_str, t):
    for fmt in ["%I:%M %p", "%H:%M"]:
        try:
            return datetime.strptime(f"{date_str} {t}", f"%Y%m%d {fmt}").replace(tzinfo=IST)
        except:
            pass
    return None


now = datetime.now(IST)
filtered_rows = []

for s in all_rows:
    st = parse_time(target_date, s["time"])
    if not st:
        s["minsLeft"] = None
        filtered_rows.append(s)
        continue

    mins = (st - now).total_seconds() / 60
    s["minsLeft"] = int(mins)

    if mins < CUT_OFF_MINUTES:
        filtered_rows.append(s)

all_rows = filtered_rows
print(f"🧹 Shows After Cutoff: {len(all_rows)}")


#########################################
# MERGE WITH PREVIOUS DATA (UPDATE LOGIC)
#########################################
prev_file = f"{OUT_DIR}/{target_date}_Detailed.json"
existing_map = {}

if os.path.exists(prev_file):
    try:
        prev_data = json.load(open(prev_file, "r"))
        for p in prev_data.get("shows", []):
            key = (p["movie"], p["venue"], p["sessionId"])
            existing_map[key] = p
    except:
        print("⚠ Previous file corrupted, ignoring...")

updated_rows = {}
reset_ignored = 0

for s in all_rows:
    key = (s["movie"], s["venue"], s["sessionId"])

    if key not in existing_map:
        updated_rows[key] = s
        continue

    prev = existing_map[key]
    new_sold = s.get("sold", 0)
    new_gross = s.get("gross", 0)

    # Condition: ignore false reset
    if new_sold == 0 and new_gross == 0:
        reset_ignored += 1
        updated_rows[key] = prev
        continue

    # Update values
    prev["sold"] = new_sold
    prev["available"] = s["available"]
    prev["gross"] = new_gross
    prev["occupancy"] = s["occupancy"]
    prev["minsLeft"] = s.get("minsLeft", prev.get("minsLeft"))
    updated_rows[key] = prev

all_rows = list(updated_rows.values())

print(f"🔄 Updated Live Show Values: {len(all_rows)}")
print(f"⛔ Ignored Reset Glitches: {reset_ignored}")


#########################################
# SUMMARY BUILD
#########################################
summary = {}
bad_fix_count = sum(1 for s in all_rows if s["badData"])

for s in all_rows:
    k = s["movie"]
    if k not in summary:
        summary[k] = {"shows": 0, "gross": 0, "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0}

    summary[k]["shows"] += 1
    summary[k]["gross"] += s["gross"]
    summary[k]["sold"] += s["sold"]
    summary[k]["totalSeats"] += s["totalSeats"]

    if 50 <= s["occupancy"] < 98:
        summary[k]["fastfilling"] += 1
    if s["occupancy"] >= 98:
        summary[k]["housefull"] += 1


#########################################
# SAVE OUTPUT
#########################################
os.makedirs(OUT_DIR, exist_ok=True)

timestamp = datetime.now(IST).strftime("%I:%M %p, %d %B %Y")

summary_json = {
    "date": target_date,
    "lastUpdated": timestamp,
    **summary
}

detail_json = {
    "date": target_date,
    "lastUpdated": timestamp,
    "resetIgnored": reset_ignored,
    "invalidAutoFixed": bad_fix_count,
    "shows": all_rows
}

summary_file = f"{OUT_DIR}/{target_date}_Summary.json"
detail_file = f"{OUT_DIR}/{target_date}_Detailed.json"

json.dump(summary_json, open(summary_file, "w"), indent=2)
json.dump(detail_json, open(detail_file, "w"), indent=2)


#########################################
# FINAL REPORT
#########################################
print("\n================================================")
print(f"🎬 Movies scraped: {len(movies)}")
print(f"🎟 Shows saved: {len(all_rows)}")
print(f"⚠ Auto-corrected API errors: {bad_fix_count}")
print(f"⛔ Ignored resets: {reset_ignored}")
print(f"📁 {summary_file}")
print(f"📁 {detail_file}")
print("================================================\n")
print("🎉 DONE — Auto Tracking Live\n")
