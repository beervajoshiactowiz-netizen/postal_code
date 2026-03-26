import threading
import requests
from lxml import html
from queue import Queue
from config import BASE_URL, HEADERS, MAX_WORKERS
from urllib.parse import urljoin
from phase1_utils import extract_links, has_codelist, save_gz
from db import get_db
import time

visited = set()
lock = threading.Lock()
task_queue = Queue()
session = requests.Session()


def is_visited(url):
    with lock:
        if url in visited:
            return True
        visited.add(url)
        return False


def insert_postal(url):
    try:
        conn = get_db()
        cursor = conn.cursor()

        parts = url.replace(BASE_URL, "").strip("/").split("/")
        country = parts[0] if len(parts) > 0 else None
        region = parts[1] if len(parts) > 1 else None
        sub = parts[2] if len(parts) > 2 else None
        sub_sub = parts[3] if len(parts) > 3 else None

        cursor.execute("""
            INSERT IGNORE INTO postal_pages
            (url, country, region, sub_region, sub_sub_region, status)
            VALUES (%s,%s,%s,%s,%s,'pending')
        """, (url, country, region, sub, sub_sub))

        conn.commit()

    except Exception as e:
        print(f"[DB ERROR] {e}")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def insert_batch(batch, retries=3):
    for attempt in range(retries):
        try:
            conn = get_db()
            cursor = conn.cursor()

            cursor.executemany("""
                INSERT IGNORE INTO postal_pages
                (url, country, region, sub_region, sub_sub_region, status)
                VALUES (%s,%s,%s,%s,%s,'pending')
            """, batch)

            conn.commit()
            return  # success → exit

        except Exception as e:
            print(f"[DB RETRY {attempt + 1}] {e}")
            time.sleep(1)  # wait before retry

        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass


def worker():
    local_batch = []

    while True:
        url = task_queue.get()

        # Stop signal
        if url is None:
            task_queue.task_done()
            break

        # Skip duplicates
        if is_visited(url):
            task_queue.task_done()
            continue

        try:
            print(f" Visiting: {url}")

            res = session.get(url, headers=HEADERS, timeout=20)
            save_gz(res.content, url)
            tree = html.fromstring(res.content)

            # ALWAYS GO DEEPER
            links = extract_links(tree, url)

            for link in links:
                if link:
                    task_queue.put(link)

            # CHECK POSTAL PAGE
            if has_codelist(tree):
                print(f"POSTAL PAGE: {url}")

                parts = url.replace(BASE_URL, "").strip("/").split("/")
                country = parts[0] if len(parts) > 0 else None
                region = parts[1] if len(parts) > 1 else None
                sub = parts[2] if len(parts) > 2 else None
                sub_sub = parts[3] if len(parts) > 3 else None

                local_batch.append((url, country, region, sub, sub_sub))

                #  BATCH INSERT
                if len(local_batch) >= 100:
                    insert_batch(local_batch)
                    local_batch.clear()

            # FALLBACK (important for edge cases)
            elif not links:
                print(f" Fallback POSTAL: {url}")

                parts = url.replace(BASE_URL, "").strip("/").split("/")
                country = parts[0] if len(parts) > 0 else None
                region = parts[1] if len(parts) > 1 else None
                sub = parts[2] if len(parts) > 2 else None
                sub_sub = parts[3] if len(parts) > 3 else None

                local_batch.append((url, country, region, sub, sub_sub))

                if len(local_batch) >= 100:
                    insert_batch(local_batch)
                    local_batch.clear()

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

        task_queue.task_done()

    # FINAL FLUSH
    if local_batch:
        insert_batch(local_batch)


def run():
    from urllib.parse import urljoin

    res = session.get(BASE_URL, headers=HEADERS)
    tree = html.fromstring(res.content)

    seen = set()
    country_urls = []

    for link in tree.xpath('//a[contains(@title,"Postal Codes")]'):
        href = link.get("href")

        if not href:
            continue

        # normalize URL
        full_url = urljoin(BASE_URL, href)


        if full_url not in seen:
            seen.add(full_url)
            country_urls.append(full_url)
    print(f" Total countries: {len(country_urls)}")

    # push to queue
    for i, url in enumerate(country_urls, 1):
        task_queue.put(url)

    threads = []
    for _ in range(MAX_WORKERS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    task_queue.join()

    for _ in threads:
        task_queue.put(None)

    for t in threads:
        t.join()

    print("Phase 1 Done")
