#!/usr/bin/env python3
import os, json, re, requests, traceback
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#########################################
#        CONFIGURATION
#########################################

BASE_URL = "https://mimorin2014.com"
SAVE_ROOT = "Japan_Data/logs"
START_YEAR = 2018
THREADS = 15
TIMEOUT = 20

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
#            SCRAPE LOGIC
#########################################

def scrape(date):
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

    for p in posts:
        title_jp = decode(p.text)
        link = BASE_URL + p["href"] if not p["href"].startswith("http") else p["href"]

        try:
            r2 = requests.get(link, timeout=TIMEOUT)
            if r2.status_code != 200:
                continue
        except:
            continue

        page = decode(BeautifulSoup(r2.text, "html.parser").title.text)

        entry_data = {
            "url": link,
            "title_jp": title_jp,
            "page_title": page,
            "rankings": []
        }

        # Updated matcher (supports new Japanese formatting)
        table_text = BeautifulSoup(r2.text, "html.parser").get_text()
        ranking_pattern = re.compile(
            r"(\d+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)\s+([\d.%]+)\s+(.+)"
        )

        for m in ranking_pattern.findall(table_text):
            rank, sales, seats, shows, theaters, ratio, movie = m
            entry_data["rankings"].append({
                "rank": int(rank),
                "movie_jp": movie.strip(),
                "sales": clean_num(sales),
                "seats": clean_num(seats),
                "showtimes": clean_num(shows),
                "theaters": clean_num(theaters),
                "ratio_last_week": clean_ratio(ratio)
            })

        result["entries"].append(entry_data)

    return result


#########################################
#         SAVE RESULTS SAFELY
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
#         MAIN EXECUTION
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

    print(f"📅 Processing {len(dates)} days...")

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        futures = {pool.submit(scrape, d): d for d in dates}

        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                data = future.result()
                if data:
                    save_json(data)
            except:
                traceback.print_exc()
                continue

    print("\n✔ Done.\n")


if __name__ == "__main__":
    run()
