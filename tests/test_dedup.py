"""Tests du dédoublonnage : empreinte url+réf+prix, annonces vues, alertes."""
from __future__ import annotations

from tests.conftest import make_listing


def test_fingerprint_stable_same_inputs():
    a = make_listing(url="https://x/1", reference="126610LN", asking_price_usd=14000)
    b = make_listing(url="https://x/1", reference="126610LN", asking_price_usd=14000)
    assert a.fingerprint == b.fingerprint


def test_fingerprint_differs_on_price():
    a = make_listing(asking_price_usd=14000)
    b = make_listing(asking_price_usd=13000)
    assert a.fingerprint != b.fingerprint


def test_fingerprint_differs_on_url():
    a = make_listing(url="https://x/1")
    b = make_listing(url="https://x/2")
    assert a.fingerprint != b.fingerprint


def test_mark_seen_only_once(db):
    ls = make_listing()
    assert db.mark_seen(ls) is True       # première fois -> nouvelle
    assert db.mark_seen(ls) is False      # déjà vue
    assert db.is_seen(ls.fingerprint) is True


def test_alert_dedup(db):
    ls = make_listing()
    assert db.is_alerted(ls.fingerprint) is False
    db.mark_alerted(ls, deal_score=0.15)
    assert db.is_alerted(ls.fingerprint) is True
    # idempotent : ne lève pas et reste alerté
    db.mark_alerted(ls, deal_score=0.15)
    assert db.is_alerted(ls.fingerprint) is True


def test_reference_normalized_lowercase_input():
    # la référence est normalisée en MAJUSCULES -> empreinte cohérente
    a = make_listing(reference="126610ln")
    b = make_listing(reference="126610LN")
    assert a.fingerprint == b.fingerprint
