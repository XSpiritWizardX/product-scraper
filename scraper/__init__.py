# This file makes the scraper folder a package
# You can also optionally expose classes for easier imports

from .multi_table_scraper import MultiTableScraper
from .url_discoverer import URLDiscoverer

__all__ = ["MultiTableScraper", "URLDiscoverer"]
