"""Connecteur Bob's Watches (scraping — pas d'API publique). Désactivé par défaut."""
from __future__ import annotations

from ._scraper import ScraperSource


class BobsWatchesSource(ScraperSource):
    name = "bobs_watches"
    search_url_template = "https://www.bobswatches.com/catalogsearch/result/?q={q}"
