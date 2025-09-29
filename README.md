# PRODUCT-SCRAPER

## Overview

**PRODUCT-SCRAPER** is a Python tool for scraping **public website data** in a secure, organized way. It dynamically detects page types (products, blogs, articles, etc.) and saves data into separate CSV files per type. The scraper maintains a **structured history** so you can track all sites that have been scraped.

> ⚠️ Only scrape public data legally. Make sure the website’s Terms of Service allow scraping.

---

## Features

- Automatically discovers all URLs on a website, including JS-heavy pages.
- Dynamically classifies pages (products, blogs, articles, others).
- Saves separate CSV files per page type.
- Maintains structured history in `data/history.json`.
- Configurable delay and timeout for slow websites.
- Per-site folders for clean data organization.
- Force-scrape option to re-run scraping for a site.

---

## Folder Structure

```css
product-scraper/
├── scraper/
│ ├── init.py
│ ├── multi_table_scraper.py
│ └── url_discoverer.py
├── data/
│ ├── history.json
│ └── <site-domain>/
│   ├── all_urls.txt
│   ├── products.csv
│   └── blogs.csv
├── run_scraper.py
├── requirements.txt
├── README.md
└── .gitignore
```




## Installation

1. Clone the repo:

```bash

git clone https://github.com/XSpiritWizardX/product-scraper.git

cd product-scraper

```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate        # Linux/Mac
# or
venv\Scripts\activate           # Windows

```

3. Install Dependencies:

```bash
pip install -r requirements.txt
python -m playwright install    # installs browsers for Playwright

```

## Usage
### 1. Configure scraping parameters
URLDiscoverer

Inside **scraper/url_discoverer.py**, adjust **delay** and **timeout** for optimal crawling speed:

```python
    class URLDiscoverer:
    def __init__(self, base_url, delay=5, max_pages=None):
        self.base_url = base_url
        self.delay = delay       # seconds between requests
        self.visited = set()
        self.max_pages = max_pages
        self.found_urls = set()

    async def fetch_page(self, page, url):
        try:
            await page.goto(url, wait_until="load", timeout=180000)  # 3 minutes max
            await asyncio.sleep(5)  # wait for JS content
            html = await page.content()
            return html
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            return None

```

## MultiTableScraper

Inside **scraper/multi_table_scraper.py**, set **delay** to optimize scraping speed:

```python
    def __init__(self, delay=5):  # seconds between page scrapes
    self.delay = delay
    self.data = {}
    self.keys = {}

```

### 2. Set the target website

Inside **run_scraper.py**, update **BASE_URL**:

```python
    BASE_URL = "https://targeted-website.com"
    DATA_FOLDER = "data"
    HISTORY_FILE = os.path.join(DATA_FOLDER, "history.json")

```
* The scraper will automatically create a folder **data/<site-domain>/**.

* CSVs for each page type are saved in this folder.

* URLs discovered are saved in **all_urls.txt**.

### 3. Run the scraper

```bash
    python run_scraper.py

```

* Already scraped sites are skipped automatically.

* To force scrape, either delete the site entry in **history.json** or set a **FORCE_SCRAPE = True** flag in **run_scraper.py**.


### 4. Output

* **data/<site-domain>/all_urls.txt** — all URLs discovered.

* **data/<site-domain>/*.csv** — one CSV per page type.

* **data/history.json** — structured record of all scraped sites:

```json
    [
  {
    "url": "https://targeted-website.com",
    "date": "2025-09-27T21:00:00",
    "pages_scraped": 120,
    "csv_files": [
      "data/targeted-website.com/products.csv",
      "data/targeted-website.com/blogs.csv"
    ]
  }
]

```


### 5. Tips for slow or JS-heavy sites

* Increase **timeout** in **URLDiscoverer.fetch_page**.

* Increase **delay** to avoid throttling.

* Set **headless=False** to debug loading issues:

```python
    browser = await p.chromium.launch(headless=False)

```

* Use small scrolls if content loads on scroll:

```python
    for _ in range(10):
    await page.evaluate("window.scrollBy(0, window.innerHeight);")
    await asyncio.sleep(1)

```

### 6. Running multiple sites

* You can duplicate **run_scraper.py** or loop through a list of URLs.

* Each site gets its own folder and CSVs automatically.


### 7. Legal & Safety

* Scraper only works on public data.

* Respect website Terms of Service.

* Avoid overloading servers — use appropriate delays.
