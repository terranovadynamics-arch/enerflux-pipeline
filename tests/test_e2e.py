"""Test bout-en-bout : scrape (fixtures) -> price -> score -> notify -> dédoublonnage.

C'est la preuve vérifiable que le système "donne des résultats" sans aucune clé
API ni accès réseau : il doit détecter exactement les bonnes affaires attendues
et ne pas réalerter au cycle suivant.
"""
from __future__ import annotations

import pytest

from dealhunter.config import get_settings


@pytest.fixture
def configured_env(tmp_path, monkeypatch):
    monkeypatch.setenv("USE_FIXTURES", "true")
    monkeypatch.setenv("NOTIFY_CHANNELS", "console")
    monkeypatch.setenv("ENABLED_SOURCES", "ebay")
    monkeypatch.setenv("MIN_COMPARABLES", "3")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "e2e.db"))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_full_cycle_detects_expected_deals(configured_env):
    from dealhunter.orchestrator import Orchestrator

    orch = Orchestrator()
    deals = orch.run_once()

    alerts = [d for d in deals if d.is_alert]
    suspicious = [d for d in deals if d.is_suspicious]

    # 3 vraies affaires (Submariner, Royal Oak, Calatrava) + 1 suspecte (arnaque).
    assert len(alerts) == 3, [f"{d.listing.reference}@{d.listing.asking_price_usd}" for d in alerts]
    assert len(suspicious) == 1
    assert suspicious[0].listing.asking_price_usd == 6900

    # Toutes les affaires sont sous le budget et au-dessus du seuil de décote.
    for d in alerts:
        assert d.listing.asking_price_usd <= 50000
        assert d.deal_score >= 0.12

    # Couverture des 3 marques cibles.
    brands = {d.listing.brand for d in alerts}
    assert brands == {"Rolex", "Audemars Piguet", "Patek Philippe"}


def test_second_cycle_dedups(configured_env):
    from dealhunter.orchestrator import Orchestrator

    orch = Orchestrator()
    first = orch.run_once()
    assert len(first) >= 1

    # Même base, mêmes annonces -> rien de neuf à alerter.
    second = orch.run_once()
    assert second == []


def test_suspicious_not_auto_validated(configured_env):
    from dealhunter.orchestrator import Orchestrator

    orch = Orchestrator()
    deals = orch.run_once()
    scam = next(d for d in deals if d.is_suspicious)
    assert scam.is_alert is False
    assert "arnaque" in scam.reason.lower()
