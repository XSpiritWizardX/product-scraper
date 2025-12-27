import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse, urldefrag
from bs4 import BeautifulSoup

class URLDiscoverer:
    def __init__(
        self,
        base_url,
        delay=0.1,
        max_pages=None,
        concurrency=5,
        wait_until="domcontentloaded",
        js_wait=0.5,
        timeout=60000,
        block_resource_types=None
    ):
        self.base_url = base_url
        self.delay = delay
        self.max_pages = max_pages
        self.concurrency = concurrency
        self.wait_until = wait_until
        self.js_wait = js_wait
        self.timeout = timeout
        if block_resource_types is None:
            self.block_resource_types = {"image", "media", "font"}
        else:
            self.block_resource_types = set(block_resource_types)
        self.visited = set()
        self.found_urls = set()
        self._state_lock = None
        self._queued = None

    async def fetch_page(self, page, url):
        try:
            await page.goto(url, wait_until=self.wait_until, timeout=self.timeout)
            if self.js_wait:
                await asyncio.sleep(self.js_wait)
            html = await page.content()
            return html
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    async def _route_block(self, route, request):
        if request.resource_type in self.block_resource_types:
            await route.abort()
        else:
            await route.continue_()

    async def _claim_visit(self, url):
        async with self._state_lock:
            if url in self.visited:
                return False
            if self.max_pages and len(self.visited) >= self.max_pages:
                return False
            self.visited.add(url)
            self._queued.discard(url)
            return True

    async def _enqueue_url(self, queue, url):
        async with self._state_lock:
            if url in self.visited or url in self._queued:
                return
            self._queued.add(url)
        await queue.put(url)

    async def _worker(self, context, queue, domain):
        page = await context.new_page()
        while True:
            url = await queue.get()
            if url is None:
                queue.task_done()
                break

            if not await self._claim_visit(url):
                queue.task_done()
                continue

            print(f"Crawling: {url}")

            html = await self.fetch_page(page, url)
            if html:
                async with self._state_lock:
                    self.found_urls.add(url)

                soup = BeautifulSoup(html, "lxml")
                for a in soup.find_all("a", href=True):
                    link, _ = urldefrag(urljoin(url, a["href"]))
                    if urlparse(link).netloc == domain:
                        await self._enqueue_url(queue, link)

            if self.delay:
                await asyncio.sleep(self.delay)

            queue.task_done()

        await page.close()

    async def crawl(self):
        domain = urlparse(self.base_url).netloc
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            if self.block_resource_types:
                await context.route("**/*", self._route_block)

            self._state_lock = asyncio.Lock()
            self._queued = set()
            queue = asyncio.Queue()
            await self._enqueue_url(queue, self.base_url)

            worker_count = self.concurrency
            tasks = [
                asyncio.create_task(self._worker(context, queue, domain))
                for _ in range(worker_count)
            ]

            await queue.join()

            for _ in range(worker_count):
                await queue.put(None)
            await asyncio.gather(*tasks, return_exceptions=True)

            await context.close()
            await browser.close()

    def save_urls(self, folder="data", filename=None):
        filename = filename or f"{urlparse(self.base_url).netloc}/all_urls.txt"
        filepath = f"{folder}/{filename}"
        with open(filepath, "w", encoding="utf-8") as f:
            for url in sorted(self.found_urls):
                f.write(url + "\n")
        print(f"✅ Saved {len(self.found_urls)} URLs to {filepath}")
