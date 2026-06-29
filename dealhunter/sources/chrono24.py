"""Connecteur Chrono24 (scraping — pas d'API publique, API réservée aux dealers).

⚠️ ToS-sensible + protection Cloudflare. Désactivé par défaut.
"""
from __future__ import annotations

from ._scraper import ScraperSource


class Chrono24Source(ScraperSource):
    name = "chrono24"
    search_url_template = "https://www.chrono24.com/search/index.htm?query={q}"
