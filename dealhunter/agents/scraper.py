"""subagent:scraper — récupère les annonces depuis chaque source.

Une instance de source par connecteur. L'échec d'une source n'interrompt pas
les autres (isolation des erreurs) ; chaque succès/erreur est journalisé.
"""
from __future__ import annotations

from ..logging_conf import get_logger
from ..models import Listing, Query
from ..sources import load_enabled_sources
from ..sources.base import Source, SourceBlocked, SourceError

log = get_logger("agent.scraper")


class ScraperAgent:
    def __init__(self, sources: list[Source] | None = None):
        self.sources = sources if sources is not None else load_enabled_sources()

    def run(self, queries: list[Query]) -> list[Listing]:
        """Exécute toutes les requêtes sur toutes les sources actives."""
        all_listings: list[Listing] = []
        for source in self.sources:
            ok, fail = 0, 0
            for query in queries:
                try:
                    listings = source.fetch(query)
                    all_listings.extend(listings)
                    ok += len(listings)
                except SourceBlocked as exc:
                    # Cas important : source bloque ou exige une clé -> on prévient.
                    fail += 1
                    log.error("⚠️ %s BLOQUÉE: %s", source.name, exc)
                    break  # inutile d'insister sur cette source pour ce cycle
                except SourceError as exc:
                    fail += 1
                    log.warning("%s: échec requête '%s': %s", source.name, query.keywords, exc)
            log.info("Source %s: %d annonces récupérées, %d échecs", source.name, ok, fail)
        log.info("Scraper: %d annonces au total ce cycle", len(all_listings))
        return all_listings
