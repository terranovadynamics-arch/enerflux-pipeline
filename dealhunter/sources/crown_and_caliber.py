"""Connecteur Crown & Caliber (scraping — pas d'API publique). Désactivé par défaut."""
from __future__ import annotations

from ._scraper import ScraperSource


class CrownAndCaliberSource(ScraperSource):
    name = "crown_and_caliber"
    search_url_template = "https://www.crownandcaliber.com/search?q={q}"
