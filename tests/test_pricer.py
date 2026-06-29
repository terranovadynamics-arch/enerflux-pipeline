"""Tests du pricer : médiane, fenêtre glissante, repli set, comparables min."""
from __future__ import annotations

from dealhunter.agents.pricer import PricerAgent, median_price
from dealhunter.config import get_settings
from dealhunter.models import SetType
from tests.conftest import make_listing


def test_median_price_odd():
    assert median_price([10, 30, 20]) == 20


def test_median_price_even():
    assert median_price([10, 20, 30, 40]) == 25


def test_median_ignores_zero_and_negative():
    assert median_price([0, -5, 100, 200]) == 150


def test_median_empty_is_none():
    assert median_price([]) is None


def test_reference_price_uses_same_set_when_enough(db):
    get_settings.cache_clear()
    pricer = PricerAgent(db)
    # 3 full-set comparables -> médiane sur le même set
    for price in (14000, 15000, 16000):
        pricer.record([make_listing(asking_price_usd=price, set=SetType.FULL,
                                    url=f"https://x/{price}")])
    target = make_listing(asking_price_usd=12000, set=SetType.FULL, url="https://x/t")
    ref, n = pricer.reference_price(target)
    assert ref == 15000
    assert n == 3


def test_reference_price_falls_back_to_all_sets(db, monkeypatch):
    monkeypatch.setenv("MIN_COMPARABLES", "3")
    get_settings.cache_clear()
    pricer = PricerAgent(db)
    # 1 seul papers-only mais 3 full-set -> repli toutes classes de set
    pricer.record([make_listing(asking_price_usd=13000, set=SetType.PAPERS_ONLY,
                                url="https://x/po")])
    for price in (14000, 15000, 16000):
        pricer.record([make_listing(asking_price_usd=price, set=SetType.FULL,
                                    url=f"https://x/{price}")])
    target = make_listing(asking_price_usd=10000, set=SetType.PAPERS_ONLY, url="https://x/t")
    ref, n = pricer.reference_price(target)
    # médiane de [13000,14000,15000,16000] = 14500
    assert ref == 14500
    assert n == 4


def test_reference_price_none_when_insufficient(db, monkeypatch):
    monkeypatch.setenv("MIN_COMPARABLES", "3")
    get_settings.cache_clear()
    pricer = PricerAgent(db)
    pricer.record([make_listing(asking_price_usd=14000, url="https://x/1")])
    target = make_listing(asking_price_usd=10000, url="https://x/t")
    ref, n = pricer.reference_price(target)
    assert ref is None
    assert n < 3
