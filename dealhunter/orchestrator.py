"""Orchestrator — agent principal.

Boucle de surveillance : planification, dédoublonnage, scoring final, déclenchement
des notifications. État persistant en SQLite -> reprise propre après crash.
"""
from __future__ import annotations

import signal
import time

from .agents.notifier import NotifierAgent
from .agents.pricer import PricerAgent
from .agents.scorer import ScorerAgent
from .agents.scraper import ScraperAgent
from .config import get_settings
from .db import Database
from .logging_conf import get_logger
from .models import Deal
from .targets import build_queries

log = get_logger("orchestrator")


class Orchestrator:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.db = Database(self.settings.db_path)
        self.scraper = ScraperAgent()
        self.pricer = PricerAgent(self.db)
        self.scorer = ScorerAgent()
        self.notifier = NotifierAgent()
        self._running = True

    # ----------------------------------------------------------------- 1 cycle
    def run_once(self) -> list[Deal]:
        """Exécute un cycle complet de surveillance et retourne les deals alertés."""
        log.info("=== Début de cycle ===")
        queries = build_queries(self.settings.budget_max_usd)

        # 1) Scraper : récupération multi-sources.
        listings = self.scraper.run(queries)

        # 2) Dédoublonnage (par empreinte url+réf+prix) -> nouvelles annonces.
        fresh = [ls for ls in listings if self.db.mark_seen(ls)]
        log.info("%d annonces nouvelles (sur %d vues)", len(fresh), len(listings))

        # 3) Pricer : on alimente l'historique avec TOUTES les annonces du cycle
        #    (pas seulement les nouvelles) pour fiabiliser la médiane marché.
        self.pricer.record(listings)

        # 4) Scorer : on évalue les nouvelles annonces uniquement.
        deals_to_alert: list[Deal] = []
        for ls in fresh:
            ref_price, comparables = self.pricer.reference_price(ls)
            deal = self.scorer.score(ls, ref_price, comparables)
            if deal is None:
                continue
            # Dédoublonnage d'alerte : jamais alertée auparavant.
            if self.db.is_alerted(ls.fingerprint):
                continue
            if deal.is_alert or deal.is_suspicious:
                deals_to_alert.append(deal)

        # 5) Notifier : digest groupé.
        if deals_to_alert:
            log.info("%d deal(s) à notifier", len(deals_to_alert))
            if self.notifier.notify(deals_to_alert):
                for deal in deals_to_alert:
                    self.db.mark_alerted(deal.listing, deal.deal_score)
        else:
            log.info("Aucun deal à notifier ce cycle")

        log.info("=== Fin de cycle ===")
        return deals_to_alert

    # -------------------------------------------------------------- boucle "24/7"
    def run_forever(self) -> None:
        """Boucle continue avec intervalle configurable + arrêt propre."""
        self._install_signal_handlers()
        interval = self.settings.poll_interval_seconds
        log.info(
            "Deal Hunter démarré. Intervalle=%d min, budget<=$%s, décote>=%.0f%%",
            self.settings.poll_interval_min,
            f"{self.settings.budget_max_usd:,.0f}",
            self.settings.discount_threshold * 100,
        )
        while self._running:
            try:
                self.run_once()
            except Exception as exc:  # un cycle planté ne tue pas le service
                log.exception("Erreur durant le cycle: %s", exc)
            # Sommeil fractionné pour réagir vite aux signaux d'arrêt.
            slept = 0
            while self._running and slept < interval:
                time.sleep(min(5, interval - slept))
                slept += 5
        log.info("Deal Hunter arrêté proprement.")

    def _install_signal_handlers(self) -> None:
        def _handler(signum, _frame):
            log.info("Signal %s reçu — arrêt en cours...", signum)
            self._running = False

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)
