from bs4 import BeautifulSoup
import csv
import os
from playwright.async_api import async_playwright
import asyncio
import hashlib
import mimetypes
from urllib.parse import urljoin, urlparse

DOWNLOAD_EXTENSIONS = {
    ".pdf", ".zip", ".rar", ".7z", ".tar", ".gz",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".csv", ".txt", ".rtf", ".json", ".xml",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".mp3", ".wav", ".m4a", ".mp4", ".mov", ".avi", ".mkv"
}

SKIP_SCHEMES = ("mailto:", "tel:", "javascript:", "data:")


class MultiTableScraper:
    def __init__(
        self,
        delay=0.1,
        site_folder=None,
        max_download_bytes=50 * 1024 * 1024,
        download_images=True,
        concurrency=5,
        wait_until="domcontentloaded",
        timeout=60000,
        extra_wait=0.5,
        block_resource_types=None
    ):
        self.delay = delay
        self.data = {}  # Scraped rows per page type
        self.keys = {}  # All keys per page type
        self.link_rows = []
        self.image_rows = []
        self.download_rows = []
        self.all_links = set()
        self.all_images = set()
        self.all_downloads = set()
        self.download_cache = {}
        self.site_folder = site_folder
        self.download_folder = os.path.join(site_folder, "downloaded_contents") if site_folder else None
        self.text_folder = os.path.join(site_folder, "page_texts") if site_folder else None
        self.max_download_bytes = max_download_bytes
        self.download_images = download_images
        self.concurrency = concurrency
        self.wait_until = wait_until
        self.timeout = timeout
        self.extra_wait = extra_wait
        if block_resource_types is None:
            self.block_resource_types = {"image", "media", "font"}
        else:
            self.block_resource_types = set(block_resource_types)
        self.text_excerpt_chars = 500
        self.max_text_chars = 20000
        self._data_lock = None
        self._download_lock = None
        self._download_tasks = None
        self._seen_urls = None
        self._seen_lock = None

    def classify_page(self, url, html, text_content, soup):
        """Detect page type using URL, text, and simple heuristics."""
        url_lower = url.lower()
        html_lower = html.lower()
        text_lower = text_content.lower()

        def has_keyword(keyword):
            return keyword in url_lower or keyword in text_lower or keyword in html_lower

        if has_keyword("course"):
            return "courses"
        if has_keyword("note"):
            return "notes"
        if has_keyword("product"):
            return "products"
        if has_keyword("blog") or "/blog" in url_lower or "/post" in url_lower:
            return "blogs"
        if has_keyword("article"):
            return "articles"
        if has_keyword("download") or has_keyword("resource"):
            return "downloads"

        image_count = len(soup.find_all("img"))
        if image_count >= 8 and len(text_content) < 400:
            return "images"

        link_count = len(soup.find_all("a"))
        if link_count >= 15 and len(text_content) < 400:
            return "links"

        return "other"

    def parse_page(self, url, soup, page_type, text_content):
        """Parse content into key-value pairs plus metadata."""
        data = {
            "URL": url,
            "PageType": page_type
        }

        # Extract dt/dd pairs
        for dt, dd in zip(soup.find_all("dt"), soup.find_all("dd")):
            key = dt.get_text(strip=True)
            if key and key not in data:
                data[key] = dd.get_text(strip=True)

        # Fallback: table rows
        if len(data) == 2:
            for row in soup.select("table tr"):
                cols = row.find_all(["th", "td"])
                if len(cols) >= 2:
                    key = cols[0].get_text(strip=True)
                    if key and key not in data:
                        data[key] = cols[1].get_text(strip=True)

        title_el = soup.find("title")
        if title_el:
            data["PageTitle"] = title_el.get_text(strip=True)

        h1_el = soup.select_one("h1")
        if h1_el:
            data["H1"] = h1_el.get_text(strip=True)

        meta_desc = soup.find("meta", attrs={"name": "description"})
        if not meta_desc:
            meta_desc = soup.find("meta", attrs={"property": "og:description"})
        if meta_desc and meta_desc.get("content"):
            data["MetaDescription"] = meta_desc["content"].strip()

        if text_content:
            data["TextExcerpt"] = text_content[:self.text_excerpt_chars]
            data["WordCount"] = len(text_content.split())
        else:
            data["WordCount"] = 0

        return data

    def normalize_url(self, base_url, candidate):
        if not candidate:
            return None
        candidate = candidate.strip()
        if candidate.startswith(SKIP_SCHEMES) or candidate.startswith("#"):
            return None
        return urljoin(base_url, candidate)

    def is_downloadable_url(self, url):
        parsed = urlparse(url)
        ext = os.path.splitext(parsed.path.lower())[1]
        return ext in DOWNLOAD_EXTENSIONS

    def _add_query_hash(self, rel_path, query):
        digest = hashlib.md5(query.encode("utf-8")).hexdigest()[:8]
        root, ext = os.path.splitext(rel_path)
        return f"{root}_{digest}{ext}"

    def url_to_relpath(self, url, content_type=None, default_name="index"):
        parsed = urlparse(url)
        path = parsed.path or ""
        if not path or path.endswith("/"):
            path = f"{path}{default_name}"

        rel_path = os.path.normpath(path.lstrip("/"))
        if rel_path in ("", "."):
            rel_path = default_name
        if rel_path.startswith(".."):
            rel_path = rel_path.replace("..", "__")

        root, ext = os.path.splitext(rel_path)
        if not ext and content_type:
            guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
            if guessed:
                ext = guessed
        rel_path = root + ext

        if parsed.query:
            rel_path = self._add_query_hash(rel_path, parsed.query)

        return rel_path

    def _ensure_unique_path(self, path):
        if not os.path.exists(path):
            return path
        base, ext = os.path.splitext(path)
        counter = 1
        while True:
            candidate = f"{base}_{counter}{ext}"
            if not os.path.exists(candidate):
                return candidate
            counter += 1

    def _relative_to_site(self, path):
        if not self.site_folder:
            return path
        return os.path.relpath(path, start=self.site_folder)

    def save_page_text(self, url, text):
        if not self.text_folder or not text:
            return ""

        rel_path = self.url_to_relpath(url, default_name="index")
        root, _ = os.path.splitext(rel_path)
        rel_path = f"{root}.txt"

        full_path = os.path.join(self.text_folder, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        trimmed = text[:self.max_text_chars]
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(trimmed)

        return self._relative_to_site(full_path)

    def collect_assets(self, page_url, soup, base_domain):
        links = []
        images = []
        downloads = []

        seen_links = set()
        for a in soup.find_all("a", href=True):
            link_url = self.normalize_url(page_url, a.get("href"))
            if not link_url or link_url in seen_links:
                continue
            seen_links.add(link_url)

            link_type = "internal"
            if base_domain and urlparse(link_url).netloc != base_domain:
                link_type = "external"

            link_text = a.get_text(strip=True)
            links.append({
                "source_url": page_url,
                "link_url": link_url,
                "link_text": link_text,
                "link_type": link_type
            })

            if a.has_attr("download") or self.is_downloadable_url(link_url):
                downloads.append((link_url, "link"))

        for img in soup.find_all("img"):
            src = img.get("src") or ""
            if not src:
                srcset = img.get("srcset", "")
                if srcset:
                    src = srcset.split(",")[0].split()[0]
            image_url = self.normalize_url(page_url, src)
            if not image_url:
                continue

            images.append({
                "source_url": page_url,
                "image_url": image_url,
                "alt_text": img.get("alt", "").strip()
            })

            if self.download_images:
                downloads.append((image_url, "image"))

        return links, images, downloads

    async def _route_block(self, route, request):
        if request.resource_type in self.block_resource_types:
            await route.abort()
        else:
            await route.continue_()

    async def _claim_url(self, url):
        if self._seen_lock is None:
            self._seen_lock = asyncio.Lock()
            self._seen_urls = set()
        async with self._seen_lock:
            if url in self._seen_urls:
                return False
            self._seen_urls.add(url)
            return True

    async def _download_content_internal(self, request_context, url):
        if not self.download_folder:
            return "", "skipped_no_folder", "", 0

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return "", "skipped_scheme", "", 0

        try:
            response = await request_context.get(url, timeout=120000)
        except Exception as e:
            return "", f"error_{e}", "", 0

        if not response.ok:
            return "", f"http_{response.status}", "", 0

        content_type = response.headers.get("content-type", "")
        content_length = response.headers.get("content-length", "")
        try:
            content_length = int(content_length)
        except (TypeError, ValueError):
            content_length = 0

        if self.max_download_bytes and content_length > self.max_download_bytes:
            return "", f"skipped_size_{content_length}", content_type, content_length

        body = await response.body()
        if self.max_download_bytes and len(body) > self.max_download_bytes:
            return "", f"skipped_body_{len(body)}", content_type, len(body)

        rel_path = self.url_to_relpath(url, content_type=content_type, default_name="download")
        full_path = os.path.join(self.download_folder, rel_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        full_path = self._ensure_unique_path(full_path)

        with open(full_path, "wb") as f:
            f.write(body)

        return self._relative_to_site(full_path), "downloaded", content_type, len(body)

    async def download_content(self, request_context, url):
        if not self.download_folder:
            return "", "skipped_no_folder", "", 0

        if self._download_lock is None:
            self._download_lock = asyncio.Lock()
            self._download_tasks = {}

        async with self._download_lock:
            cached = self.download_cache.get(url)
            if cached:
                return cached
            existing = self._download_tasks.get(url)
            if existing is None:
                task = asyncio.create_task(self._download_content_internal(request_context, url))
                self._download_tasks[url] = task
                existing = task

        result = await existing

        async with self._download_lock:
            self.download_cache[url] = result
            self._download_tasks.pop(url, None)

        return result

    async def _worker(self, context, queue, base_domain):
        page = await context.new_page()
        while True:
            url = await queue.get()
            if url is None:
                queue.task_done()
                break

            if not await self._claim_url(url):
                queue.task_done()
                continue

            print(f"Scraping: {url}")
            try:
                await page.goto(url, wait_until=self.wait_until, timeout=self.timeout)
                if self.extra_wait:
                    await asyncio.sleep(self.extra_wait)
                html = await page.content()
            except Exception as e:
                print(f"Failed to scrape {url}: {e}")
                queue.task_done()
                continue

            soup = BeautifulSoup(html, "lxml")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text_content = " ".join(soup.stripped_strings)

            page_type = self.classify_page(url, html, text_content, soup)
            page_data = self.parse_page(url, soup, page_type, text_content)

            links, images, downloads = self.collect_assets(url, soup, base_domain)
            local_all_links = {link["link_url"] for link in links}
            local_all_images = {image["image_url"] for image in images}
            local_all_downloads = {download_url for download_url, _ in downloads}

            downloaded_paths = []
            local_download_rows = []
            for download_url, download_kind in downloads:
                saved_path, status, content_type, file_size = await self.download_content(context.request, download_url)
                local_download_rows.append({
                    "source_url": url,
                    "download_url": download_url,
                    "download_kind": download_kind,
                    "saved_path": saved_path,
                    "status": status,
                    "content_type": content_type,
                    "file_size": file_size
                })
                if saved_path:
                    downloaded_paths.append(saved_path)

            page_data["LinkCount"] = len(links)
            page_data["ImageCount"] = len(images)
            page_data["DownloadCount"] = len(downloads)
            if downloaded_paths:
                page_data["DownloadedFiles"] = " | ".join(downloaded_paths)

            text_path = self.save_page_text(url, text_content)
            if text_path:
                page_data["TextFile"] = text_path

            async with self._data_lock:
                if page_type not in self.data:
                    self.data[page_type] = []
                    self.keys[page_type] = set()
                self.data[page_type].append(page_data)
                self.keys[page_type].update(page_data.keys())
                if links:
                    self.link_rows.extend(links)
                    self.all_links.update(local_all_links)
                if images:
                    self.image_rows.extend(images)
                    self.all_images.update(local_all_images)
                if local_download_rows:
                    self.download_rows.extend(local_download_rows)
                if local_all_downloads:
                    self.all_downloads.update(local_all_downloads)

            if self.delay:
                await asyncio.sleep(self.delay)

            queue.task_done()

        await page.close()

    async def scrape_urls(self, urls):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()

            if self.block_resource_types:
                await context.route("**/*", self._route_block)

            self._data_lock = asyncio.Lock()
            self._download_lock = asyncio.Lock()
            self._download_tasks = {}
            self._seen_lock = asyncio.Lock()
            self._seen_urls = set()

            base_domain = ""
            if urls:
                base_domain = urlparse(urls[0]).netloc

            if self.download_folder:
                os.makedirs(self.download_folder, exist_ok=True)
            if self.text_folder:
                os.makedirs(self.text_folder, exist_ok=True)

            queue = asyncio.Queue()
            for url in urls:
                await queue.put(url)

            worker_count = min(self.concurrency, len(urls)) if urls else 0
            tasks = [
                asyncio.create_task(self._worker(context, queue, base_domain))
                for _ in range(worker_count)
            ]

            await queue.join()

            for _ in range(worker_count):
                await queue.put(None)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            await context.close()
            await browser.close()

    def _write_csv(self, rows, filename):
        if not rows:
            return
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())
        fieldnames = list(all_keys)

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                for key in fieldnames:
                    if key not in row:
                        row[key] = ""
                writer.writerow(row)

    def _write_list(self, items, filename):
        if not items:
            return
        with open(filename, "w", encoding="utf-8") as f:
            for item in sorted(items):
                f.write(item + "\n")

    def save_csvs(self, folder="data"):
        """Save one CSV per detected page type plus links, images, and downloads."""
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

        self._write_csv(self.link_rows, os.path.join(folder, "links.csv"))
        self._write_csv(self.image_rows, os.path.join(folder, "images.csv"))
        self._write_csv(self.download_rows, os.path.join(folder, "downloads.csv"))

        self._write_list(self.all_links, os.path.join(folder, "all_links.txt"))
        self._write_list(self.all_images, os.path.join(folder, "all_images.txt"))
        self._write_list(self.all_downloads, os.path.join(folder, "all_downloads.txt"))
