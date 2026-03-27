from parser import run
from db import setup_database
from config import BASE_URL, HEADERS
from urllib.parse import urljoin
import requests
from lxml import html
import time

def main():
    setup_database()

    # Get country URLs from homepage
    res  = requests.get(BASE_URL, headers=HEADERS)
    tree = html.fromstring(res.content)

    seen         = set()
    country_urls = []

    for link in tree.xpath('//a[contains(@title,"Postal Codes")]'):
        href     = link.get("href")
        full_url = urljoin(BASE_URL, href)
        path     = full_url.replace(BASE_URL, "").strip("/")

        # Only country level (no slash in path)
        if "/" not in path and full_url not in seen:
            seen.add(full_url)
            country_urls.append(full_url)

    print(f"Total countries: {len(country_urls)}")

    # Start scraping
    run(start_urls=country_urls)

if __name__ == "__main__":
    st=time.time()
    main()
    et=time.time()
    print(et-st)
