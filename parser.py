import requests
from lxml import html
from urllib.parse import urljoin
import os, gzip
import threading
from queue import Queue
from config import BASE_URL, HEADERS, MAX_WORKERS, BUFFER_LIMIT
from db import (
    insert_countries, insert_regions,
    insert_subregions, insert_sub_subregions,
    insert_postal
)

visited      = set()
visited_lock = threading.Lock()
task_queue   = Queue()
session      = requests.Session()
session.headers.update(HEADERS)

#Batch buffers per level
buffers = {
    "countries":       [],
    "regions":         [],
    "sub_regions":     [],
    "sub_sub_regions": [],
    "postal":          [],
}
buffer_lock = threading.Lock()


def save_gz(content, url):
    path  = url.replace(BASE_URL, "").strip("/")
    depth = len(path.split("/")) if path else 0
    folder = {
        1: "countries",
        2: "regions",
        3: "sub_regions",
        4: "sub_sub_regions"
    }.get(depth, "others")

    os.makedirs(f"storage/{folder}", exist_ok=True)
    name = path.replace("/", "_")
    file = f"storage/{folder}/{name}.html.gz"

    if not os.path.exists(file):
        with gzip.open(file, "wb") as f:
            f.write(content)


def get_depth(url):
    path = url.replace(BASE_URL, "").strip("/")
    return len(path.split("/")) if path else 0


def is_visited(url):
    with visited_lock:
        if url in visited:
            return True
        visited.add(url)
        return False


def extract_links(tree, current_url):
    links  = tree.xpath('//div[@class="regions"]//a/@href')
    result = []
    seen   = set()
    for l in links:
        full = urljoin(BASE_URL, l)
        if get_depth(full) == get_depth(current_url) + 1 and full not in seen:
            seen.add(full)
            result.append(full)
    return result


def is_postal(tree):
    return bool(
        tree.xpath('//div[@class="codes"]//span')
    )

def flush_buffer(key):
    with buffer_lock:
        data = buffers[key].copy()
        buffers[key].clear()

    if not data:
        return

    if key == "countries":
        insert_countries(data)
    elif key == "regions":
        insert_regions(data)
    elif key == "sub_regions":
        insert_subregions(data)
    elif key == "sub_sub_regions":
        insert_sub_subregions(data)
    elif key == "postal":
        insert_postal(data)

    print(f"DB Flushed {len(data)} → {key}")

def add_to_buffer(key, row):
    with buffer_lock:
        buffers[key].append(row)
        should_flush = len(buffers[key]) >= BUFFER_LIMIT

    if should_flush:
        flush_buffer(key)

def flush_all():
    for key in buffers:
        flush_buffer(key)


def worker():
    while True:
        url = task_queue.get()

        if url is None:
            task_queue.task_done()
            break

        if is_visited(url):
            task_queue.task_done()
            continue

        try:
            print(f"{url}")

            res  = session.get(url, timeout=20)
            tree = html.fromstring(res.content)
            save_gz(res.content, url)

            links = extract_links(tree, url)

            parts   = url.replace(BASE_URL, "").strip("/").split("/")
            country = parts[0] if len(parts) > 0 else None
            region  = parts[1] if len(parts) > 1 else None
            sub     = parts[2] if len(parts) > 2 else None
            sub_sub = parts[3] if len(parts) > 3 else None
            depth   = get_depth(url)

            # Insert to correct level
            if depth == 1:
                add_to_buffer("countries", (country, url, "done"))
            elif depth == 2:
                add_to_buffer("regions", (country, region, url, "done"))
            elif depth == 3:
                add_to_buffer("sub_regions", (country, region, sub, url, "done"))
            elif depth == 4:
                add_to_buffer("sub_sub_regions", (country, region, sub, sub_sub, url, "done"))

            # Postal check
            if is_postal(tree):
                add_to_buffer("postal", (country, region, sub, sub_sub, url, "pending"))
                print(f"POSTAL: {url}")
            elif not links:
                add_to_buffer("postal", (country, region, sub, sub_sub, url, "pending"))
                print(f"FALLBACK POSTAL: {url}")

            # ── Push sub-links to queue ──
            # ← NO visited check here, let worker handle it
            for link in links:
                task_queue.put(link)

        except Exception as e:
            print(f"{url} → {e}")

        task_queue.task_done()

def run(start_urls=None):
    if start_urls is None:
        start_urls = [BASE_URL]

    for url in start_urls:
        task_queue.put(url)

    threads = []
    for _ in range(MAX_WORKERS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    task_queue.join()

    for _ in threads:
        task_queue.put(None)
    for t in threads:
        t.join()

    flush_all()
    print("Done")