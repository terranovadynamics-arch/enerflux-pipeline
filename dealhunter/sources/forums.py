"""Connecteur forums "for sale" (WatchUSeek, etc. — XenForo, scraping).

Désactivé par défaut. Le contenu des forums est bruité : à filtrer fortement.
"""
from __future__ import annotations

from ._scraper import ScraperSource


class ForumsSource(ScraperSource):
    name = "forums"
    search_url_template = (
        "https://www.watchuseek.com/search/?q={q}&c[nodes][0]=forsale"
    )
