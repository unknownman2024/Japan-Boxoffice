#!/usr/bin/env python3
import os, json, re, requests, traceback, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

#########################################
#        CONFIGURATION
#########################################

BASE_URL = "https://mimorin2014.com"
SAVE_ROOT = "Japan_Data/logs"
START_YEAR = 2018
THREADS = 100
TIMEOUT = 20
RETRIES = 2

#########################################
#             UTILITIES
#########################################

def decode(text):
    try:
        return BeautifulSoup(text, "html.parser").text.strip()
    except:
        return text.strip()

def clean_num(value):
    value = re.sub(r"[^0-9]", "", value)
    return int(value) if value else 0

def clean_ratio(value):
    value = re.sub(r"[^0-9.]", "", value)
    return float(value) if value else None

def exists(date):
    y, m = date.split("-")[0:2]
    return os.path.exists(f"{SAVE_ROOT}/{y}/{m}/{date}.json")


#########################################
#        SCRAPE SINGLE DAY
#########################################

def scrape_day(date):
    formatted = date.replace("-", "")
    url = f"{BASE_URL}/blog-date-{formatted}.html"

    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
    except:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    posts = soup.select("h2.entry_header a")

    if not posts:
        return None

    result = {"date": date, "entries": []}

    for post in posts:
        title_jp = decode(post.text)
        link = BASE_URL + post["href"] if not post["href"].startswith("http") else post["href"]

        try:
            r2 = requests.get(link, timeout=TIMEOUT)
            if r2.status_code != 200:
                continue
        except:
            continue

        page = decode(BeautifulSoup(r2.text, "html.parser").title.text)

        entry = {
            "url": link,
            "title_jp": title_jp,
            "page_title": page,
            "rankings": []
        }

        text = BeautifulSoup(r2.text, "html.parser").get_text()

        ranking_pattern = re.compile(
            r"(\d+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d.%]+)\s+(.+)"
        )

        for match in ranking_pattern.findall(text):
            rank, sales, seats, shows, theaters, ratio, movie = match
            entry["rankings"].append({
                "rank": int(rank),
                "movie_jp": movie.strip(),
                "sales": clean_num(sales),
                "seats": clean_num(seats),
                "showtimes": clean_num(shows),
                "theaters": clean_num(theaters),
                "ratio_last_week": clean_ratio(ratio)
            })

        result["entries"].append(entry)

    return result


#########################################
#        RETRY WRAPPER
#########################################

def scrape(date):
    attempts = RETRIES

    while attempts >= 0:
        try:
            return scrape_day(date)
        except Exception as e:
            attempts -= 1
            time.sleep(1)

    with open("error.log", "a", encoding="utf-8") as f:
        f.write(f"{date} FAILED after retries\n")

    return None


#########################################
#         SAVE RESULT
#########################################

def save_json(data):
    y, m = data["date"].split("-")[0:2]
    path = f"{SAVE_ROOT}/{y}/{m}"
    os.makedirs(path, exist_ok=True)

    file = f"{path}/{data['date']}.json"

    try:
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except:
        traceback.print_exc()


#########################################
#           MAIN EXECUTION
#########################################

def run():
    jp_now = datetime.utcnow() + timedelta(hours=9)
    today = jp_now.strftime("%Y-%m-%d")

    start = datetime(START_YEAR, 1, 1)
    end = jp_now

    dates = []
    while start <= end:
        d = start.strftime("%Y-%m-%d")
        if not exists(d) or d == today:
            dates.append(d)
        start += timedelta(days=1)

    print(f"📅 Total missing days: {len(dates)}")

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for data in tqdm(pool.map(scrape, dates), total=len(dates)):
            if data:
                save_json(data)

    print("\n✔ Done (All days processed)\n")


if __name__ == "__main__":
    run()
