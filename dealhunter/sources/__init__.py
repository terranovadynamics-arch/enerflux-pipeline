"""Registre des sources : déclare ici tout nouveau connecteur.

Pour ajouter une source : créez un module exposant une classe `Source`,
puis enregistrez-la dans `ALL_SOURCES`. Voir sources/README.md.
"""
from __future__ import annotations

from ..config import get_settings
from ..logging_conf import get_logger
from .base import Source
from .bobs_watches import BobsWatchesSource
from .chrono24 import Chrono24Source
from .crown_and_caliber import CrownAndCaliberSource
from .ebay import EbaySource
from .forums import ForumsSource
from .subdial import SubdialSource
from .watchbox import WatchBoxSource
from .watchfinder import WatchfinderSource

log = get_logger("sources")

# Catalogue complet des connecteurs disponibles (un par site).
ALL_SOURCES: dict[str, type[Source]] = {
    EbaySource.name: EbaySource,
    Chrono24Source.name: Chrono24Source,
    BobsWatchesSource.name: BobsWatchesSource,
    WatchfinderSource.name: WatchfinderSource,
    CrownAndCaliberSource.name: CrownAndCaliberSource,
    WatchBoxSource.name: WatchBoxSource,
    SubdialSource.name: SubdialSource,
    ForumsSource.name: ForumsSource,
}


def load_enabled_sources() -> list[Source]:
    """Instancie les sources listées dans ENABLED_SOURCES (.env)."""
    settings = get_settings()
    sources: list[Source] = []
    for name in settings.enabled_sources:
        cls = ALL_SOURCES.get(name.strip())
        if cls is None:
            log.warning("Source inconnue ignorée: %s", name)
            continue
        sources.append(cls())
        log.info(
            "Source activée: %s (API officielle=%s)",
            name,
            cls.has_official_api,
        )
    if not sources:
        log.warning("Aucune source activée ! Vérifiez ENABLED_SOURCES.")
    return sources
