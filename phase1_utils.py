import os
import gzip
from urllib.parse import urljoin
from config import BASE_URL

def get_depth(url):
    path = url.replace(BASE_URL, "").strip("/")
    return len(path.split("/")) if path else 0

def get_folder(url):
    return {
        1: "countries",
        2: "regions",
        3: "sub_regions",
        4: "sub_sub_regions",
    }.get(get_depth(url), "sub_sub_regions")

def save_gz(content, url):
    folder = get_folder(url)
    base = "storage/" + folder
    os.makedirs(base, exist_ok=True)

    name = url.replace(BASE_URL, "").strip("/").replace("/", "_")
    path = os.path.join(base, f"{name}.html.gz")

    if not os.path.exists(path):
        with gzip.open(path, "wb", compresslevel=1) as f:
            f.write(content)

from urllib.parse import urljoin

def extract_links(tree, current_url):
    current_depth = get_depth(current_url)

    links = tree.xpath('//div[@class="regions"]//a/@href')

    if not links:
        links = tree.xpath('//a[starts-with(@href, "/")]/@href')

    seen = set()
    results = []

    for l in links:


        if not l or not isinstance(l, str):
            continue

        if not l.startswith("/"):
            continue


        full = urljoin(current_url, l)
        full = full.replace("//", "/").replace("https:/", "https://")
        full = full.rstrip("/")


        if not full.startswith(BASE_URL):
            continue


        if get_depth(full) <= current_depth:
            continue

        if full not in seen:
            seen.add(full)
            results.append(full)

    return results

def has_codelist(tree):
    return bool(tree.xpath('//h2[text()="Codes List"]'))
