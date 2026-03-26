# import threading
# import requests
# import time
# from lxml import html
# from queue import Queue
# from db import get_db
# from config import MAX_WORKERS, BATCH_SIZE, FETCH_LIMIT
#
# task_queue = Queue()
# session = requests.Session()
#
# def extract_data(tree):
#     areas = tree.xpath('//div[@class="place"]/text()')
#     pincodes = tree.xpath('//div[@class="code"]/span/text()')
#
#     return list(zip(
#         [a.strip() for a in areas],
#         [p.strip() for p in pincodes]
#     ))
#
# def fetch_batch():
#     conn = get_db()
#     cursor = conn.cursor()
#
#     cursor.execute(f"""
#         UPDATE postal_pages
#         SET status='processing'
#         WHERE status='pending'
#         LIMIT {FETCH_LIMIT}
#     """)
#     conn.commit()
#
#     cursor.execute("""
#         SELECT url, country, region, sub_region, sub_sub_region
#         FROM postal_pages
#         WHERE status='processing'
#     """)
#
#     rows = cursor.fetchall()
#     cursor.close()
#     conn.close()
#
#     return rows
#
# def mark_done(url):
#     conn = get_db()
#     cursor = conn.cursor()
#     cursor.execute("UPDATE postal_pages SET status='done' WHERE url=%s", (url,))
#     conn.commit()
#     cursor.close()
#     conn.close()
#
# def flush_batch(batch):
#     conn = get_db()
#     cursor = conn.cursor()
#
#     cursor.executemany("""
#         INSERT INTO postal_data
#         (country, region, sub_region, sub_sub_region, area, pincode)
#         VALUES (%s,%s,%s,%s,%s,%s)
#     """, batch)
#
#     conn.commit()
#     cursor.close()
#     conn.close()
#
# def worker():
#     local_batch = []
#
#     while True:
#         item = task_queue.get()
#         if item is None:
#             break
#
#         url, country, region, sub, sub_sub = item
#
#         try:
#             res = session.get(url, timeout=20)
#             tree = html.fromstring(res.content)
#
#             for area, pin in extract_data(tree):
#                 local_batch.append((country, region, sub, sub_sub, area, pin))
#
#             mark_done(url)
#
#             if len(local_batch) >= BATCH_SIZE:
#                 flush_batch(local_batch)
#                 local_batch.clear()
#
#         except:
#             pass
#
#         task_queue.task_done()
#
#     if local_batch:
#         flush_batch(local_batch)
#
# def run():
#     while True:
#         rows = fetch_batch()
#
#         if not rows:
#             print(" Waiting for new URLs...")
#             time.sleep(5)
#             continue
#
#         for r in rows:
#             task_queue.put(r)
#
#         threads = []
#         for _ in range(MAX_WORKERS):
#             t = threading.Thread(target=worker)
#             t.start()
#             threads.append(t)
#
#         task_queue.join()
#
#         for _ in threads:
#             task_queue.put(None)
#
#         for t in threads:
#             t.join()


import threading
import requests
import time
from lxml import html
from queue import Queue
from db import get_db
from config import MAX_WORKERS, BATCH_SIZE, FETCH_LIMIT


task_queue = Queue()
session = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}
session.headers.update(HEADERS)


def fetch_with_retry(url, retries=3):
    for i in range(retries):
        try:
            res = session.get(url, timeout=20)

            if res.status_code != 200 or not res.content.strip():
                raise Exception("Empty response")

            return html.fromstring(res.content)

        except Exception as e:
            print(f" Retry {i+1}: {url}")
            time.sleep(0.5)

    print(f" Failed: {url}")
    return None


def extract_units(tree):
    return tree.xpath('//div[contains(@class,"container")]')


def flush_batch(batch):
    conn = get_db()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT IGNORE INTO postal_data
        (country, region, sub_region, sub_sub_region, area, pincode)
        VALUES (%s,%s,%s,%s,%s,%s)
    """, batch)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted {len(batch)} rows")

def mark_done_batch(urls):
    conn = get_db()
    cursor = conn.cursor()

    cursor.executemany(
        "UPDATE postal_pages SET status='done' WHERE url=%s",
        [(u,) for u in urls]
    )

    conn.commit()
    cursor.close()
    conn.close()


def fetch_batch():
    conn = get_db()
    cursor = conn.cursor()

    # mark limited rows as processing
    cursor.execute(f"""
        UPDATE postal_pages
        SET status='processing'
        WHERE status='pending'
        LIMIT {FETCH_LIMIT}
    """)
    conn.commit()

    cursor.execute("""
        SELECT url, country, region, sub_region, sub_sub_region
        FROM postal_pages
        WHERE status='processing'
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows


def worker():
    local_batch = []
    done_urls = []

    while True:
        item = task_queue.get()

        if item is None:
            task_queue.task_done()
            break

        url, country, region, sub, sub_sub = item

        try:
            print(f"{url}")

            tree = fetch_with_retry(url)

            if tree is None:
                task_queue.task_done()
                continue

            units = extract_units(tree)

            if not units:
                print(f" No data: {url}")
                task_queue.task_done()
                continue

            for unit in units:
                area = unit.xpath('.//div[@class="place"]/text()')
                pin = unit.xpath('.//div[@class="code"]/span/text()')

                if area and pin:
                    local_batch.append((
                        country, region, sub, sub_sub,
                        area[0].strip(),
                        pin[0].strip()
                    ))

            done_urls.append(url)


            if len(local_batch) >= BATCH_SIZE:
                flush_batch(local_batch)
                local_batch.clear()


            if len(done_urls) >= 100:
                mark_done_batch(done_urls)
                done_urls.clear()

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

        task_queue.task_done()


    if local_batch:
        flush_batch(local_batch)

    if done_urls:
        mark_done_batch(done_urls)


def run():
    # start threads ONCE
    threads = []
    for _ in range(MAX_WORKERS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    while True:
        rows = fetch_batch()

        if not rows:
            print("Waiting for new URLs...")
            time.sleep(5)
            continue

        print(f"Processing {len(rows)} URLs")

        for r in rows:
            task_queue.put(r)

        task_queue.join()


if __name__ == "__main__":
    run()