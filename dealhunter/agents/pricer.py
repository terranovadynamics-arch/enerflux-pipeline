"""subagent:pricer — établit le prix de référence marché (médiane) par référence.

Prix de référence = médiane des annonces actives comparables (même réf, set
similaire) sur une fenêtre glissante de N jours, toutes sources confondues.
L'historique est conservé en base pour suivre la tendance.
"""
from __future__ import annotations

import statistics

from ..config import get_settings
from ..db import Database
from ..logging_conf import get_logger
from ..models import Listing

log = get_logger("agent.pricer")


def median_price(prices: list[float]) -> float | None:
    """Médiane robuste d'une liste de prix (None si vide)."""
    clean = [p for p in prices if p and p > 0]
    if not clean:
        return None
    return float(statistics.median(clean))


class PricerAgent:
    def __init__(self, db: Database):
        self.db = db
        self.settings = get_settings()

    def record(self, listings: list[Listing]) -> None:
        """Alimente l'historique de prix avec les annonces du cycle."""
        for ls in listings:
            self.db.record_price(ls)

    def reference_price(self, listing: Listing) -> tuple[float | None, int]:
        """Retourne (prix_référence, nb_comparables) pour une annonce.

        Stratégie :
          1. comparables même référence + même classe de set ;
          2. si trop peu (< MIN_COMPARABLES), repli sur toutes classes de set ;
          3. si toujours trop peu, retourne (None, n) -> pas de scoring fiable.
        """
        window = self.settings.price_window_days
        min_n = self.settings.min_comparables

        same_set = self.db.prices_for(listing.reference, listing.set_class, window)
        if len(same_set) >= min_n:
            return median_price(same_set), len(same_set)

        all_sets = self.db.prices_for(listing.reference, None, window)
        if len(all_sets) >= min_n:
            log.debug(
                "%s: repli toutes classes de set (%d comparables)",
                listing.reference, len(all_sets),
            )
            return median_price(all_sets), len(all_sets)

        return None, len(all_sets)

    def trend(self, listing: Listing) -> list[tuple]:
        """Série temporelle de prix pour la référence (suivi de tendance)."""
        return self.db.price_trend(
            listing.reference, None, self.settings.price_window_days
        )
