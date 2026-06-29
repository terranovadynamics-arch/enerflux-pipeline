"""Connecteur Watchfinder (Richemont — scraping, anti-bot). Désactivé par défaut."""
from __future__ import annotations

from ._scraper import ScraperSource


class WatchfinderSource(ScraperSource):
    name = "watchfinder"
    search_url_template = "https://www.watchfinder.com/search?q={q}"
