#!/usr/bin/env python3
import os, json, re, requests, traceback, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

#########################################
#        CONFIGURATION
#########################################

BASE_URL = "https://mimorin2014.com"
SAVE_ROOT = "Japan Data/logs"
START_YEAR = 2018
THREADS = 20
TIMEOUT = 15

#########################################
#  SAFE TRANSLATION: FALLBACK ALWAYS WORKS
#########################################

try:
    from transformers import MarianMTModel, MarianTokenizer
    print("Loading translation model...")
    tokenizer = MarianTokenizer.from_pretrained("Helsinki-NLP/opus-mt-ja-en")
    model = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-ja-en")
    MODEL_AVAILABLE = True
except:
    MODEL_AVAILABLE = False
    print("⚠ Translation model unavailable. Using fallback ONLY.")

CACHE_FILE = "translation_cache.json"

if os.path.exists(CACHE_FILE):
    translation_cache = json.load(open(CACHE_FILE,"r",encoding="utf-8"))
else:
    translation_cache = {}

def save_cache():
    try:
        with open(CACHE_FILE,"w",encoding="utf-8") as f:
            json.dump(translation_cache,f,ensure_ascii=False,indent=2)
    except:
        pass


def translate(text):
    """Safe Translation - never breaks, always returns something."""
    text = text.strip()

    if not text:
        return text
    
    if len(text) > 120:  # avoid model overload
        return text

    if text in translation_cache:
        return translation_cache[text]

    if not MODEL_AVAILABLE:
        translation_cache[text] = text
        return text

    try:
        tokens = tokenizer([text], return_tensors="pt", padding=True, truncation=True)
        out = model.generate(**tokens)
        eng = tokenizer.decode(out[0], skip_special_tokens=True)
        translation_cache[text] = eng
        return eng
    except:
        translation_cache[text] = text
        return text


#########################################
#             UTILITIES
#########################################

def decode(text):
    try:
        return BeautifulSoup(text, "html.parser").text
    except:
        return text

def clean_num(value):
    try:
        return int(re.sub(r"[^0-9]", "", value))
    except:
        return 0

def clean_ratio(value):
    try:
        cleaned = re.sub(r"[^0-9.]", "", value)
        return float(cleaned) if cleaned else None
    except:
        return None

def exists(date):
    y,m = date.split("-")[0:2]
    return os.path.exists(f"{SAVE_ROOT}/{y}/{m}/{date}.json")


#########################################
#            SCRAPE LOGIC
#########################################

def scrape(date):
    formatted = date.replace("-","")
    url = f"{BASE_URL}/blog-date-{formatted}.html"

    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
    except:
        return None
    
    html = r.text

    posts = re.findall(
        r'<h2 class="entry_header">\s*<a href="([^"]+)"[^>]*>(.*?)<\/a>',
        html, re.S
    )

    result = {"date":date,"entries":[]}

    for link, title_jp in posts:
        link = f"{BASE_URL}/{link}"
        try:
            r2 = requests.get(link, timeout=TIMEOUT)
        except:
            continue

        if r2.status_code!=200:
            continue

        details = r2.text

        title_match = re.search(r"<title>(.*?)<\/title>", details)
        full_title_jp = decode(title_match.group(1)) if title_match else ""

        entry_data = {
            "url": link,
            "title_jp": title_jp.strip(),
            "title_en": translate(title_jp.strip()),
            "page_title_jp": full_title_jp,
            "page_title_en": translate(full_title_jp),
            "rankings":[]
        }

        ranking_pattern = re.compile(r"(\d+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d*]+)\s+([\d.%*]+)\s+(.+)")
        
        for m in ranking_pattern.findall(details):
            try:
                rank, sales, seats, shows, theaters, ratio, movie_jp = m
                
                entry_data["rankings"].append({
                    "rank": int(rank),
                    "movie_jp": movie_jp.strip(),
                    "movie_en": translate(movie_jp.strip()),
                    "sales": clean_num(sales),
                    "seats": clean_num(seats),
                    "showtimes": clean_num(shows),
                    "theaters": clean_num(theaters),
                    "ratio_last_week": clean_ratio(ratio)
                })
            except:
                continue
        
        result["entries"].append(entry_data)

    return result


#########################################
#         SAVE RESULTS SAFELY
#########################################

def save_json(data):
    y,m = data["date"].split("-")[0:2]
    path = f"{SAVE_ROOT}/{y}/{m}"
    os.makedirs(path, exist_ok=True)

    file = f"{path}/{data['date']}.json"

    today = (datetime.utcnow()+timedelta(hours=9)).strftime("%Y-%m-%d")

    if os.path.exists(file) and data["date"]!=today:
        return  # skip old

    try:
        with open(file,"w",encoding="utf-8") as f:
            json.dump(data,f,ensure_ascii=False,indent=2)
    except:
        pass


#########################################
#         MAIN EXECUTION
#########################################

def run():
    jp_now = datetime.utcnow()+timedelta(hours=9)
    today = jp_now.strftime("%Y-%m-%d")

    start = datetime(START_YEAR,1,1)
    end = jp_now

    dates=[]
    while start<=end:
        d = start.strftime("%Y-%m-%d")
        if not exists(d) or d==today:
            dates.append(d)
        start+=timedelta(days=1)

    print(f"📅 Need to process: {len(dates)} days\n")

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

    save_cache()
    print("\n✔ Completed.\n")


if __name__ == "__main__":
    run()
