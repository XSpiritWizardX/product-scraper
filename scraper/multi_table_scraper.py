from bs4 import BeautifulSoup
import csv
import os
from playwright.async_api import async_playwright
import asyncio
import time

class MultiTableScraper:
    def __init__(self, delay=5):
        self.delay = delay
        self.data = {}  # Scraped rows per page type
        self.keys = {}  # All keys per page type

    def classify_page(self, html):
        """Automatically detect page type from common identifiers."""
        html_lower = html.lower()
        if 'product' in html_lower:
            return "products"
        elif 'blog' in html_lower:
            return "blogs"
        elif 'article' in html_lower:
            return "articles"
        else:
            return "other"

    def parse_page(self, html, page_type):
        """Parse content into key-value pairs."""
        soup = BeautifulSoup(html, "lxml")
        data = {}

        # Extract dt/dd pairs
        for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
            data[dt.get_text(strip=True)] = dd.get_text(strip=True)

        # Fallback: table rows
        if not data:
            for row in soup.select("table tr"):
                cols = row.find_all(["th", "td"])
                if len(cols) >= 2:
                    data[cols[0].get_text(strip=True)] = cols[1].get_text(strip=True)

        # Optional: page title
        title_el = soup.select_one("h1")
        if title_el:
            data["Title"] = title_el.get_text(strip=True)

        return data

    async def scrape_urls(self, urls):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            for url in urls:
                print(f"Scraping: {url}")
                try:
                    await page.goto(url, wait_until="networkidle")
                    html = await page.content()
                except Exception as e:
                    print(f"Failed to scrape {url}: {e}")
                    continue

                page_type = self.classify_page(html)
                if page_type not in self.data:
                    self.data[page_type] = []
                    self.keys[page_type] = set()

                page_data = self.parse_page(html, page_type)
                if page_data:
                    self.data[page_type].append(page_data)
                    self.keys[page_type].update(page_data.keys())

                await asyncio.sleep(self.delay)

            await browser.close()

    def save_csvs(self, folder="data"):
        """Save one CSV per detected page type automatically."""
        os.makedirs(folder, exist_ok=True)
        for page_type, rows in self.data.items():
            if not rows:
                continue
            all_keys = list(self.keys[page_type])
            for row in rows:
                for key in all_keys:
                    if key not in row:
                        row[key] = ""
            filename = os.path.join(folder, f"{page_type}.csv")
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_keys)
                writer.writeheader()
                writer.writerows(rows)
            print(f"✅ Saved {len(rows)} rows to {filename}")
