#!/usr/bin/env python3
import os, json, re, requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tqdm import tqdm

# --------- Translation Model ----------
from transformers import MarianMTModel, MarianTokenizer

print("Loading translation model...")
model_name = "Helsinki-NLP/opus-mt-ja-en"
tokenizer = MarianTokenizer.from_pretrained(model_name)
model = MarianMTModel.from_pretrained(model_name)

CACHE_FILE = "translation_cache.json"

# Load translation cache
if os.path.exists(CACHE_FILE):
    translation_cache = json.load(open(CACHE_FILE, "r", encoding="utf-8"))
else:
    translation_cache = {}

def save_cache():
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(translation_cache, f, ensure_ascii=False, indent=2)

def translate(text):
    text = text.strip()

    # Skip long junk text
    if len(text) > 120:
        return text

    if text in translation_cache:
        return translation_cache[text]

    try:
        batch = tokenizer([text], return_tensors="pt", padding=True, truncation=True)
        output = model.generate(**batch)
        translated = tokenizer.decode(output[0], skip_special_tokens=True)
        translation_cache[text] = translated
        return translated
    except:
        translation_cache[text] = text
        return text

def decode(text):
    return BeautifulSoup(text, "html.parser").text


# --------- Scraper Logic ----------

BASE_URL = "https://mimorin2014.com"
SAVE_ROOT = "Japan Data/logs"

def scrape_day(date_str):
    formatted = date_str.replace("-", "")
    url = f"{BASE_URL}/blog-date-{formatted}.html"

    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
    except:
        return None

    html = r.text
    entries = re.findall(r'<h2 class="entry_header">\s*<a href="([^"]+)"[^>]*>(.*?)<\/a>', html, re.S)

    day_result = {"date": date_str, "entries": []}

    for link, title_jp in entries:
        full_url = f"{BASE_URL}/{link}"

        try:
            sub = requests.get(full_url, timeout=20)
        except:
            continue

        if sub.status_code != 200:
            continue

        detail = sub.text

        title_match = re.search(r"<title>(.*?)<\/title>", detail)
        full_title_jp = decode(title_match.group(1)) if title_match else ""

        # Prepare for parallel translation
        title_short_en = translate(title_jp)
        full_title_en = translate(full_title_jp)

        ranking_regex = re.compile(
            r"(\d+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d.%*]+)\s+(.+)"
        )

        rankings = []
        for m in ranking_regex.findall(detail):
            rank, sales, seats, shows, theaters, ratio, movie_jp = m

            def clean(x): return int(x.replace("*", "")) if x else 0

            rankings.append({
                "rank": int(rank),
                "movie_jp": movie_jp.strip(),
                "movie_en": translate(movie_jp.strip()),
                "sales": clean(sales),
                "seats": clean(seats),
                "showtimes": clean(shows),
                "theaters": clean(theaters),
                "last_week_ratio": None if ratio == "******" else float(ratio.replace("%", ""))
            })

        day_result["entries"].append({
            "url": full_url,
            "title_jp": title_jp.strip(),
            "title_en": title_short_en,
            "page_title_jp": full_title_jp,
            "page_title_en": full_title_en,
            "rankings": rankings
        })

    return day_result


def write_result(data):
    year, month = data["date"].split("-")[0:2]
    path = f"{SAVE_ROOT}/{year}/{month}"
    os.makedirs(path, exist_ok=True)

    file = f"{path}/{data['date']}.json"

    today = datetime.utcnow() + timedelta(hours=9)
    today = today.strftime("%Y-%m-%d")

    if os.path.exists(file) and data["date"] != today:
        return

    with open(file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return file


def already_exists(date_str):
    year, month = date_str.split("-")[0:2]
    return os.path.exists(f"{SAVE_ROOT}/{year}/{month}/{date_str}.json")


def run():
    start_year = 2018
    today_jp = datetime.utcnow() + timedelta(hours=9)
    today = today_jp.strftime("%Y-%m-%d")

    start = datetime(start_year, 1, 1)
    end = today_jp

    dates_to_process = []

    while start <= end:
        d = start.strftime("%Y-%m-%d")
        if not already_exists(d) or d == today:
            dates_to_process.append(d)
        start += timedelta(days=1)

    print(f"📅 Pending scrape count: {len(dates_to_process)}")

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(scrape_day, d): d for d in dates_to_process}

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                write_result(result)

    save_cache()
    print("✔ Done.")


if __name__ == "__main__":
    run()
