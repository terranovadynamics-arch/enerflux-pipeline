"""Tests du scorer : logique de décote, seuils, anti-arnaque, vendeur."""
from __future__ import annotations

import pytest

from dealhunter.agents.scorer import (
    ScorerAgent,
    compute_deal_score,
    seller_is_credible,
)
from dealhunter.config import get_settings
from tests.conftest import make_listing


# --- deal_score = (ref - asking) / ref --------------------------------------
def test_compute_deal_score_basic():
    assert compute_deal_score(10000, 8000) == pytest.approx(0.20)


def test_compute_deal_score_no_discount():
    assert compute_deal_score(10000, 10000) == 0.0


def test_compute_deal_score_above_market_negative():
    assert compute_deal_score(10000, 12000) == pytest.approx(-0.20)


def test_compute_deal_score_zero_reference_is_safe():
    assert compute_deal_score(0, 5000) == 0.0


# --- crédibilité vendeur -----------------------------------------------------
def test_seller_low_feedback_rejected():
    assert seller_is_credible(make_listing(seller_feedback=80.0)) is False


def test_seller_new_account_rejected():
    assert seller_is_credible(
        make_listing(seller_feedback=None, seller_sales_count=2)
    ) is False


def test_seller_unknown_is_neutral():
    # Pas de donnée vendeur (boutique/forum) -> considéré crédible.
    assert seller_is_credible(
        make_listing(seller_feedback=None, seller_sales_count=None)
    ) is True


# --- décision d'alerte -------------------------------------------------------
def test_alert_triggered_on_good_deal():
    agent = ScorerAgent()
    ls = make_listing(asking_price_usd=12600)  # ref 14800 -> ~14.9%
    deal = agent.score(ls, reference_price=14800, comparables=5)
    assert deal is not None
    assert deal.is_alert is True
    assert deal.is_suspicious is False
    assert deal.discount_pct == pytest.approx(14.9, abs=0.1)


def test_no_deal_below_threshold_returns_none():
    agent = ScorerAgent()
    ls = make_listing(asking_price_usd=14000)  # ref 14800 -> ~5.4% < 12%
    assert agent.score(ls, reference_price=14800, comparables=5) is None


def test_over_budget_not_alerted():
    agent = ScorerAgent()
    # décote 20% mais prix > budget 50k -> pas d'alerte
    ls = make_listing(asking_price_usd=56000)
    deal = agent.score(ls, reference_price=70000, comparables=5)
    assert deal is None  # over budget -> écarté


def test_suspicious_flag_on_abnormal_discount():
    agent = ScorerAgent()
    ls = make_listing(asking_price_usd=6900)  # ref 14800 -> ~53% > 45%
    deal = agent.score(ls, reference_price=14800, comparables=5)
    assert deal is not None
    assert deal.is_suspicious is True
    assert deal.is_alert is False  # pas validé automatiquement
    assert "arnaque" in deal.reason.lower()


def test_no_reference_price_no_deal():
    agent = ScorerAgent()
    ls = make_listing(asking_price_usd=10000)
    assert agent.score(ls, reference_price=None, comparables=0) is None


def test_threshold_is_configurable(monkeypatch):
    monkeypatch.setenv("DISCOUNT_THRESHOLD", "0.30")
    get_settings.cache_clear()
    agent = ScorerAgent()
    ls = make_listing(asking_price_usd=12600)  # ~14.9% < 30%
    assert agent.score(ls, reference_price=14800, comparables=5) is None
