import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time

class URLDiscoverer:
    def __init__(self, base_url, delay=1, max_pages=None):
        self.base_url = base_url
        self.delay = delay
        self.visited = set()
        self.max_pages = max_pages
        self.found_urls = set()

    async def fetch_page(self, page, url):
        try:
            await page.goto(url, wait_until="load", timeout=180000)
            await asyncio.sleep(5)  # wait 5 seconds for JS content
            html = await page.content()
            return html
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    async def crawl_page(self, page, url, domain):
        if url in self.visited or (self.max_pages and len(self.visited) >= self.max_pages):
            return
        self.visited.add(url)
        print(f"Crawling: {url}")

        html = await self.fetch_page(page, url)
        if html is None:
            return

        self.found_urls.add(url)

        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a['href'])
            if urlparse(link).netloc == domain:
                await self.crawl_page(page, link, domain)

        await asyncio.sleep(self.delay)

    async def crawl(self):
        domain = urlparse(self.base_url).netloc
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await self.crawl_page(page, self.base_url, domain)
            await browser.close()

    def save_urls(self, folder="data", filename=None):
        filename = filename or f"{urlparse(self.base_url).netloc}/all_urls.txt"
        filepath = f"{folder}/{filename}"
        with open(filepath, "w", encoding="utf-8") as f:
            for url in sorted(self.found_urls):
                f.write(url + "\n")
        print(f"✅ Saved {len(self.found_urls)} URLs to {filepath}")
