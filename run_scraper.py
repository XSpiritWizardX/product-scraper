import asyncio
import json
import os
from datetime import datetime
from urllib.parse import urlparse
from scraper.url_discoverer import URLDiscoverer
from scraper.multi_table_scraper import MultiTableScraper

# Change this to the site you want to scrape
BASE_URL = "https://stainlesssteelkitchen.com"
DATA_FOLDER = "data"
HISTORY_FILE = os.path.join(DATA_FOLDER, "history.json")

async def main():
    os.makedirs(DATA_FOLDER, exist_ok=True)

    # Load existing history
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r") as f:
            history = json.load(f)
    else:
        history = []

    # Skip if site already scraped
    if any(entry["url"] == BASE_URL for entry in history):
        print(f"⚠️ Already scraped {BASE_URL}, skipping...")
        return

    # Create a subfolder for this site
    site_folder = os.path.join(DATA_FOLDER, urlparse(BASE_URL).netloc)
    os.makedirs(site_folder, exist_ok=True)

    # Step 1: Discover URLs
    discoverer = URLDiscoverer(BASE_URL)
    await discoverer.crawl()
    discoverer.save_urls(folder=DATA_FOLDER)

    # Copy all_urls.txt to site folder
    src_urls = os.path.join(DATA_FOLDER, f"{urlparse(BASE_URL).netloc}/all_urls.txt")
    dest_urls = os.path.join(site_folder, "all_urls.txt")
    if os.path.exists(src_urls):
        os.replace(src_urls, dest_urls)
    total_pages = len(open(dest_urls).readlines())

    # Step 2: Scrape content dynamically
    with open(dest_urls, "r") as f:
        urls = [line.strip() for line in f.readlines()]

    scraper = MultiTableScraper()
    await scraper.scrape_urls(urls)
    scraper.save_csvs(folder=site_folder)

    # Collect CSV filenames
    csv_files = [f"{site_folder}/{filename}" for filename in os.listdir(site_folder) if filename.endswith(".csv")]

    # Step 3: Record structured history
    site_entry = {
        "url": BASE_URL,
        "date": datetime.now().isoformat(),
        "pages_scraped": total_pages,
        "csv_files": csv_files
    }
    history.append(site_entry)

    with open(HISTORY_FILE, "w") as f:
        json.dump(history, f, indent=4)

    print(f"✅ Added {BASE_URL} to structured history")

if __name__ == "__main__":
    asyncio.run(main())
