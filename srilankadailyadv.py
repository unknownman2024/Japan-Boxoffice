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
SCRAPE_PASSES = 7
TIMEOUT_SEC = 1

OUT_DIR = "Sri Lanka Advance"
IST = ZoneInfo("Asia/Kolkata")


#########################################
# RANDOM HEADERS (Anti-Bot)
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
# SCRAPER ENGINE
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
# API FUNCTIONS
#########################################
def get_movies():
    url = "https://lk.bookmyshow.com/pwa/api/uapi/movies/"
    body = {"regionCode": "SNLK", "page": 1, "limit": 200, "filters": {}}
    return safe_request(url, "POST", body)


def get_showtimes(event_code, date):
    url = f"https://lk.bookmyshow.com/pwa/api/de/showtimes/byevent?regionCode=SNLK&eventCode={event_code}&dateCode={date}"
    return safe_request(url)


#########################################
# JSON PARSING HELPERS
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
# BUILD FINAL ENTRY FOR EACH SHOW
#########################################
def flatten(movie_obj, venue, sh, date):
    session_id = sh.get("SessionId") or sh.get("Id") or ""
    total = sum(int(c.get("MaxSeats", 0)) for c in sh.get("Categories", []))
    avail = sum(int(c.get("SeatsAvail", 0)) for c in sh.get("Categories", []))
    price = float(sh.get("MinPrice", 0))

    sold = total - avail
    gross = sold * price
    occupancy = round((sold / total * 100), 2) if total else 0

    bad = False
    if sold < 0 or gross < 0 or avail > total or total == 0:
        sold, gross, occupancy = 0, 0, 0
        bad = True

    return {
        "movie": movie_obj["title"],
        "format": movie_obj["format"],
        "language": movie_obj["language"],
        "eventCode": movie_obj["eventCode"],
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
# SCRAPE SINGLE EVENTCODE
#########################################
def scrape_event(movie, date, attempt):
    title = f"{movie['title']} ({movie['format'] or 'Std'})"
    print(f"➡ Fetching: {title} | Code: {movie['eventCode']} | Lang: {movie['language']}")

    res, err = get_showtimes(movie["eventCode"], date)
    if not res:
        print(f"❌ FAIL → {title} | {err}")
        return [], False

    venues = extract_venues(res, date)
    if not venues:
        print(f"⚠ NO SHOWS → {title}")
        return [], False

    rows = []
    for v in venues:
        for sh in v.get("ShowTimes", []):
            rows.append(flatten(movie, v, sh, date))

    print(f"✅ SUCCESS → {title} ({len(rows)} shows)")
    return rows, True



#########################################
# START SCRIPT
#########################################
print("\n🚀 Sri Lanka Advance Booking Scraper Started...\n")

target_date = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

movies_raw, _ = get_movies()
parent_movies = extract_movies(movies_raw)

#########################################
# EXPAND MOVIES INTO FORMAT VARIANTS
#########################################
expanded_movies = []

for movie in parent_movies:
    for c in movie.get("ChildEvents", []):
        expanded_movies.append({
            "title": movie["EventTitle"],
            "eventCode": c["EventCode"],
            "format": c.get("EventDimension", ""),
            "language": c.get("EventLanguage", ""),
            "release": c.get("EventDate", "9999-12-31"),
        })

print(f"🎬 Parent Titles: {len(parent_movies)}")
print(f"🎭 Total Event Variants: {len(expanded_movies)}\n")


#########################################
# MULTIPASS SCRAPING ENGINE
#########################################
all_rows = []
pending = expanded_movies.copy()

for attempt in range(1, SCRAPE_PASSES + 1):

    if not pending:
        break

    print(f"\n🔁 PASS {attempt}/{SCRAPE_PASSES} — retrying {len(pending)} pending...\n")
    next_round = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        tasks = {pool.submit(scrape_event, m, target_date, attempt): m for m in pending}

        for job in as_completed(tasks):
            rows, ok = job.result()
            if ok:
                all_rows.extend(rows)
            else:
                next_round.append(tasks[job])

    pending = next_round


#########################################
# DEDUPE BY (eventCode + venue + sessionId)
#########################################
deduped = {(s["eventCode"], s["venue"], s["sessionId"]): s for s in all_rows}
all_rows = list(deduped.values())

#########################################
# SUMMARY BUILD (NO MERGE, NO OLD DATA)
#########################################
summary = {}
bad_fix_count = sum(1 for s in all_rows if s["badData"])

for s in all_rows:
    title = s["movie"]
    event = s["eventCode"]

    if title not in summary:
        summary[title] = {"formats": {}}

    if event not in summary[title]["formats"]:
        summary[title]["formats"][event] = {
            "format": s["format"],
            "language": s["language"],
            "shows": 0,
            "gross": 0,
            "sold": 0,
            "totalSeats": 0
        }

    f = summary[title]["formats"][event]
    f["shows"] += 1
    f["gross"] += s["gross"]
    f["sold"] += s["sold"]
    f["totalSeats"] += s["totalSeats"]


#########################################
# SAVE FILES
#########################################
os.makedirs(OUT_DIR, exist_ok=True)

timestamp = datetime.now(IST).strftime("%I:%M %p, %d %B %Y")

summary_file = f"{OUT_DIR}/{target_date}_Summary.json"
detail_file = f"{OUT_DIR}/{target_date}_Detailed.json"

json.dump({"date": target_date, "lastUpdated": timestamp, "movies": summary}, open(summary_file, "w"), indent=2)
json.dump({"date": target_date, "lastUpdated": timestamp, "shows": all_rows, "autoFixed": bad_fix_count}, open(detail_file, "w"), indent=2)


#########################################
# FINAL LOG
#########################################
print("\n================================================")
print(f"🎥 Variants Scraped: {len(expanded_movies)}")
print(f"📊 Advance Shows: {len(all_rows)}")
print(f"⚠ Auto-corrected invalid API seats: {bad_fix_count}")
print(f"📁 Saved: {summary_file}")
print(f"📁 Saved: {detail_file}")
print("================================================")
print("🎉 Done — Advance Tracking Enabled\n")
