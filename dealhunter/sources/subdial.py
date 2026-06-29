"""Connecteur Subdial (index de prix surtout, pas d'API de listings). Désactivé par défaut."""
from __future__ import annotations

from ._scraper import ScraperSource


class SubdialSource(ScraperSource):
    name = "subdial"
    search_url_template = "https://subdial.com/search?q={q}"
