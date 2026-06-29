"""subagent:scorer — calcule le deal_score et décide s'il faut alerter.

deal_score = (prix_référence - asking_price) / prix_référence  (en fraction).

ALERTE si TOUTES ces conditions sont vraies :
  - asking_price_usd <= BUDGET_MAX_USD
  - deal_score >= DISCOUNT_THRESHOLD (12% par défaut)
  - vendeur crédible (feedback OK, pas de red flag évident)
  - décote pas anormale (sinon -> flag "À VÉRIFIER, possible arnaque")
Le dédoublonnage (annonce jamais alertée) est géré en amont par l'orchestrateur.
"""
from __future__ import annotations

from ..config import get_settings
from ..logging_conf import get_logger
from ..models import Deal, Listing

log = get_logger("agent.scorer")


def compute_deal_score(reference_price: float, asking_price: float) -> float:
    """Décote relative. Positive = en dessous du marché."""
    if reference_price <= 0:
        return 0.0
    return (reference_price - asking_price) / reference_price


def seller_is_credible(listing: Listing) -> bool:
    """Heuristique anti-arnaque sur le vendeur (red flags évidents).

    - feedback < 95% => suspect ;
    - très peu de ventes (< 10) ET feedback inconnu/bas => suspect.
    Les sources sans donnée vendeur sont considérées neutres (crédibles) afin
    de ne pas écarter les boutiques/forums qui n'exposent pas de score.
    """
    fb = listing.seller_feedback
    sales = listing.seller_sales_count

    if fb is not None and fb < 95.0:
        return False
    if sales is not None and sales < 10 and (fb is None or fb < 98.0):
        return False
    return True


class ScorerAgent:
    def __init__(self):
        self.settings = get_settings()

    def score(
        self, listing: Listing, reference_price: float | None, comparables: int
    ) -> Deal | None:
        """Retourne un Deal si l'annonce mérite attention, sinon None.

        Retourne None quand il n'y a pas de prix de référence fiable
        (pas assez de comparables) — on ne peut pas juger une affaire à l'aveugle.
        """
        if reference_price is None or reference_price <= 0:
            return None

        score = compute_deal_score(reference_price, listing.asking_price_usd)
        within_budget = listing.asking_price_usd <= self.settings.budget_max_usd
        meets_discount = score >= self.settings.discount_threshold
        suspicious = score > self.settings.scam_threshold
        seller_ok = seller_is_credible(listing)

        # On alerte si dans le budget, décote suffisante, vendeur crédible,
        # et décote pas anormale (l'anormale est signalée séparément).
        is_alert = within_budget and meets_discount and seller_ok and not suspicious

        if not (meets_discount and within_budget):
            return None  # rien d'intéressant -> on n'encombre pas

        reason = self._build_reason(
            listing, reference_price, score, suspicious, seller_ok
        )
        return Deal(
            listing=listing,
            reference_price_usd=round(reference_price, 2),
            deal_score=round(score, 4),
            is_alert=is_alert,
            is_suspicious=suspicious,
            seller_ok=seller_ok,
            reason=reason,
            comparables_count=comparables,
        )

    @staticmethod
    def _build_reason(
        listing: Listing,
        ref: float,
        score: float,
        suspicious: bool,
        seller_ok: bool,
    ) -> str:
        pct = round(score * 100, 1)
        saving = round(ref - listing.asking_price_usd)
        base = (
            f"{pct}% sous le prix de référence marché "
            f"(~${ref:,.0f}), soit ~${saving:,} d'économie."
        )
        if suspicious:
            return "⚠️ À VÉRIFIER, possible arnaque — " + base + (
                " Décote anormalement élevée : vérifiez l'authenticité, le vendeur "
                "et les photos avant tout contact."
            )
        if not seller_ok:
            return "⚠️ Vendeur à vérifier — " + base
        return "Bonne affaire : " + base
