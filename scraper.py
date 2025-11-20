#!/usr/bin/env python3
import os
import json
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from transformers import MarianMTModel, MarianTokenizer

# ----------------------------------------
# Translation Model (Offline)
# ----------------------------------------
print("Loading translation model...")
model_name = "Helsinki-NLP/opus-mt-ja-en"
tokenizer = MarianTokenizer.from_pretrained(model_name)
model = MarianMTModel.from_pretrained(model_name)

def translate(text):
    text = text.strip()
    if not text:
        return text
    try:
        tokens = tokenizer([text], return_tensors="pt", padding=True)
        output = model.generate(**tokens)
        return tokenizer.decode(output[0], skip_special_tokens=True)
    except:
        return text

def decode_html_entities(text):
    return BeautifulSoup(text, "html.parser").text


# ----------------------------------------
# Scraper Core Functions
# ----------------------------------------

BASE_URL = "https://mimorin2014.com"

def scrape_day(date_str):
    """Scrapes one specific date: YYYY-MM-DD"""

    formatted = date_str.replace("-", "")
    url = f"{BASE_URL}/blog-date-{formatted}.html"

    resp = requests.get(url)
    if resp.status_code != 200:
        return None

    html = resp.text

    # extract blog links
    entries = re.findall(
        r'<h2 class="entry_header">\s*<a href="([^"]+)"[^>]*>(.*?)<\/a>',
        html, re.S
    )

    out = {"date": date_str, "entries": []}

    for link, title_jp in entries:
        full_url = f"{BASE_URL}/{link}"

        sub = requests.get(full_url)
        if sub.status_code != 200:
            continue

        detail = sub.text

        # Title
        t_match = re.search(r"<title>(.*?)<\/title>", detail)
        title_full_jp = decode_html_entities(t_match.group(1)) if t_match else ""

        title_full_en = translate(title_full_jp)
        title_short_en = translate(title_jp.strip())

        # Extract rankings
        ranking_regex = re.compile(
            r"(\d+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d.%*]+)\s+(.+)"
        )

        rankings = []

        for m in ranking_regex.findall(detail):
            rank, sales, seats, shows, theaters, ratio, movie_jp = m

            def clean(x): return int(x.replace("*", "")) if x else 0
            movie_en = translate(movie_jp.strip())

            try:
                ratio_val = float(ratio.replace("%", "")) if ratio != "******" else None
            except:
                ratio_val = None

            rankings.append({
                "rank": int(rank),
                "movie_jp": movie_jp.strip(),
                "movie_en": movie_en,
                "sales": clean(sales),
                "seats": clean(seats),
                "showtimes": clean(shows),
                "theaters": clean(theaters),
                "last_week_ratio": ratio_val
            })

        out["entries"].append({
            "url": full_url,
            "title_jp": title_jp.strip(),
            "title_en": title_short_en,
            "page_title_jp": title_full_jp,
            "page_title_en": title_full_en,
            "rankings": rankings
        })

    return out


# ----------------------------------------
# File Management & Execution
# ----------------------------------------

SAVE_ROOT = "Japan Data/logs"

def save_day(data):
    year = data["date"].split("-")[0]
    month = data["date"].split("-")[1]
    filename = f"{SAVE_ROOT}/{year}/{month}/{data['date']}.json"

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    # Only overwrite IF it's today
    today = datetime.now().strftime("%Y-%m-%d")

    if os.path.exists(filename) and data["date"] != today:
        return  # skip old files

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return filename


def file_exists(date_str):
    year = date_str.split("-")[0]
    month = date_str.split("-")[1]
    filename = f"{SAVE_ROOT}/{year}/{month}/{date_str}.json"
    return os.path.exists(filename)


# ----------------------------------------
# Main Processing Logic
# ----------------------------------------

def run_full_scrape(start_year=2018):
    today_jp = datetime.utcnow() + timedelta(hours=9)  # JST time
    today = today_jp.strftime("%Y-%m-%d")

    # create full date range
    start_date = datetime(start_year, 1, 1)
    end_date = today_jp

    dates = []
    while start_date <= end_date:
        d = start_date.strftime("%Y-%m-%d")

        # skip if exists and not today
        if not file_exists(d) or d == today:
            dates.append(d)

        start_date += timedelta(days=1)

    print(f"\n📅 Days requiring fetch: {len(dates)}")

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(scrape_day, d): d for d in dates}

        for future in tqdm(as_completed(futures), total=len(futures)):
            data = future.result()
            if data:
                save_day(data)

    print("\n✔ Completed.")


if __name__ == "__main__":
    run_full_scrape(2018)
