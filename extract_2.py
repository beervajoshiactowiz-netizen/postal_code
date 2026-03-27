import requests
from lxml import html
from concurrent.futures import ThreadPoolExecutor
import threading
from db import get_postal_urls, insert_postal_data, update_postal_status

HEADERS  = {"User-Agent": "Mozilla/5.0"}

MAX_WORKERS  = 20
BATCH_SIZE   = 500

batch        = []
batch_lock   = threading.Lock()
done_urls    = []
done_lock    = threading.Lock()

def flush_batch():
    with batch_lock:
        if not batch:
            return
        data = batch.copy()
        batch.clear()
    insert_postal_data(data)
    print(f"[DB] Inserted {len(data)} rows")

def flush_done():
    with done_lock:
        if not done_urls:
            return
        urls = done_urls.copy()
        done_urls.clear()
    update_postal_status(urls)
    print(f"[DB] Marked {len(urls)} done")

def extract(row):
    country, region, sub, sub_sub, url = row

    try:
        res  = requests.get(url, headers=HEADERS, timeout=20)
        tree = html.fromstring(res.content)

        units = tree.xpath('//div[contains(@class,"unit")]')

        local = []
        for u in units:
            area = u.xpath('string(.//div[contains(@class,"place")])').strip()
            code = u.xpath('string(.//div[@class="code"]/span)').strip()
            if area and code:
                local.append((country, region, sub, sub_sub, area, code))

        if local:
            with batch_lock:
                batch.extend(local)
                should_flush = len(batch) >= BATCH_SIZE
            if should_flush:
                flush_batch()

        with done_lock:
            done_urls.append(url)
            should_flush_done = len(done_urls) >= 100
        if should_flush_done:
            flush_done()

        print(f"{url} → {len(local)} rows")

    except Exception as e:
        print(f"{url} → {e}")


def run():
    while True:
        rows = get_postal_urls(limit=500)

        if not rows:
            print("ALL DONE")
            break

        print(f"Processing {len(rows)} URLs")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            ex.map(extract, rows)

        # Flush remaining after each batch
        flush_batch()
        flush_done()

if __name__ == "__main__":
    run()