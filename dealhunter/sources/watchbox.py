"""Connecteur WatchBox / The Art Of Time (scraping). Désactivé par défaut."""
from __future__ import annotations

from ._scraper import ScraperSource


class WatchBoxSource(ScraperSource):
    name = "watchbox"
    search_url_template = "https://www.thewatchbox.com/search?q={q}"
