import cloudscraper
import random
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


#########################################
# CONFIG
#########################################
MAX_THREADS = 5
RETRY_PER_REQUEST = 6
SCRAPE_PASSES = 8
TIMEOUT_SEC = 1
CUT_OFF_MINUTES = 200  # keep only near shows

OUT_DIR = "Sri Lanka Boxoffice"


#########################################
# RANDOM USER AGENT / HEADERS
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
            r = scraper.post(url, json=payload, headers=headers, timeout=TIMEOUT_SEC) if method=="POST" \
                else scraper.get(url, headers=headers, timeout=TIMEOUT_SEC)

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
# DATA HELPERS
#########################################
def extract_movies(raw):
    if not raw:
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


def flatten(movie, venue, sh, date):
    total = sum(int(c.get("MaxSeats", 0)) for c in sh.get("Categories", []))
    avail = sum(int(c.get("SeatsAvail", 0)) for c in sh.get("Categories", []))
    sold = total - avail
    price = float(sh.get("MinPrice", 0))

    # Fix invalid API negative data
    if sold < 0 or avail > total or total == 0:
        sold = 0
        avail = 0 if total == 0 else avail
        gross = 0
        occupancy = 0
        badData = True
    else:
        gross = sold * price
        occupancy = round((sold / total * 100), 2) if total else 0
        badData = False

    return {
        "movie": movie,
        "venue": venue.get("VenueName"),
        "time": sh.get("ShowTime"),
        "sessionId": str(sh.get("SessionId")),
        "totalSeats": total,
        "available": avail,
        "sold": sold,
        "gross": gross,
        "occupancy": occupancy,
        "date": date,
        "badData": badData
    }


#########################################
# SCRAPE MOVIE
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
# START
#########################################
print("\n🚀 Sri Lanka Showtimes Scraper Started...\n")

target_date = (datetime.now()).strftime("%Y%m%d")

movies_raw, _ = get_movies()
movies = [m for m in extract_movies(movies_raw) if "EventTitle" in m]

print(f"🎞 Total Movies Found: {len(movies)}\n")

all_rows = []
pending = movies.copy()


#########################################
# MULTI-PASS FETCH (Retry System)
#########################################
for attempt in range(1, SCRAPE_PASSES + 1):
    if not pending:
        break

    print(f"\n🔁 PASS {attempt}/{SCRAPE_PASSES} → {len(pending)} movies...")

    next_pending = []

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
        tasks = {pool.submit(scrape_movie, m, target_date, attempt): m for m in pending}

        for job in as_completed(tasks):
            _, rows, ok = job.result()
            if ok:
                all_rows.extend(rows)
            else:
                next_pending.append(tasks[job])

    pending = next_pending


#########################################
# DEDUPE USING SESSIONID
#########################################
seen = {}
for s in all_rows:
    key = (s["movie"], s["venue"], s["sessionId"])
    seen[key] = s  # last write wins

all_rows = list(seen.values())


#########################################
# CUT-OFF FILTER BASED ON CURRENT TIME
#########################################
def parse_show_time(date, time_str):
    for fmt in ["%I:%M %p", "%H:%M"]:
        try:
            return datetime.strptime(f"{date} {time_str}", f"%Y%m%d {fmt}")
        except:
            pass
    return None


filtered = []
now = datetime.now()

for s in all_rows:
    st = parse_show_time(target_date, s["time"])
    if not st:
        filtered.append(s)
        continue

    mins_left = (st - now).total_seconds() / 60
    s["minsLeft"] = int(mins_left)

    if mins_left <= CUT_OFF_MINUTES:
        filtered.append(s)

all_rows = filtered


#########################################
# MERGE WITH PREVIOUS DATA (UPDATE LOGIC)
#########################################
existing_path = f"{OUT_DIR}/{target_date}_Detailed.json"
existing_data = {}

if os.path.exists(existing_path):
    try:
        existing_data = json.load(open(existing_path, "r"))
        existing_rows = {
            (s["movie"], s["venue"], s["sessionId"]): s
            for s in existing_data.get("shows", [])
        }
    except:
        existing_rows = {}
else:
    existing_rows = {}

updated_rows = {}
fix_reset_ignored = 0

for s in all_rows:
    key = (s["movie"], s["venue"], s["sessionId"])

    if key in existing_rows:
        old = existing_rows[key]

        new_sold = s.get("sold", 0)
        new_gross = s.get("gross", 0)

        # Condition: if new reset to 0, DO NOT UPDATE
        if new_sold == 0 and new_gross == 0:
            fix_reset_ignored += 1
            updated_rows[key] = old
        else:
            # Always replace updated values
            old["sold"] = new_sold
            old["available"] = s["available"]
            old["gross"] = new_gross
            old["occupancy"] = s["occupancy"]
            old["minsLeft"] = s.get("minsLeft", old.get("minsLeft"))
            updated_rows[key] = old
    else:
        updated_rows[key] = s

all_rows = list(updated_rows.values())


#########################################
# SUMMARY BUILD
#########################################
summary = {}

for s in all_rows:
    k = s["movie"]
    if k not in summary:
        summary[k] = {"shows": 0, "gross": 0, "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0}

    data = summary[k]
    data["shows"] += 1
    data["gross"] += s["gross"]
    data["sold"] += s["sold"]
    data["totalSeats"] += s["totalSeats"]

    occ = s["occupancy"]
    if 50 <= occ < 98: data["fastfilling"] += 1
    if occ >= 98: data["housefull"] += 1


#########################################
# SAVE FILES
#########################################
os.makedirs(OUT_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%I:%M %p, %d %B %Y")

final_summary = {"date": target_date, "lastUpdated": timestamp, **summary}
final_detailed = {"date": target_date, "lastUpdated": timestamp, "ignoredZeroUpdates": fix_reset_ignored, "shows": all_rows}

summary_path = f"{OUT_DIR}/{target_date}_Summary.json"
detailed_path = f"{OUT_DIR}/{target_date}_Detailed.json"

json.dump(final_summary, open(summary_path, "w"), indent=2)
json.dump(final_detailed, open(detailed_path, "w"), indent=2)

print("\n💾 Saved Files:")
print(f"📁 {summary_path}")
print(f"📁 {detailed_path}")

print(f"\n⚠ Ignored Zero-reset Attempts: {fix_reset_ignored}")
print("\n🎉 DONE — Sri Lanka Showtime Tracking Complete\n")
