"""Fixtures de tests partagées."""
from __future__ import annotations

import pytest

from dealhunter.config import Settings, get_settings
from dealhunter.db import Database


@pytest.fixture(autouse=True)
def _reset_settings_cache():
    """Repart d'une config par défaut propre pour chaque test."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        BUDGET_MAX_USD=50000,
        DISCOUNT_THRESHOLD=0.12,
        SCAM_THRESHOLD=0.45,
        PRICE_WINDOW_DAYS=30,
        MIN_COMPARABLES=3,
    )


@pytest.fixture
def db(tmp_path) -> Database:
    return Database(str(tmp_path / "test.db"))


def make_listing(**kw):
    """Helper : crée un Listing avec des valeurs par défaut raisonnables."""
    from dealhunter.models import Listing, SetType

    defaults = dict(
        brand="Rolex",
        model="Submariner",
        reference="126610LN",
        year=2022,
        condition="very good",
        set=SetType.FULL,
        asking_price_usd=14000,
        seller="trusted_dealer",
        seller_feedback=99.5,
        seller_sales_count=500,
        location="US",
        url="https://example.com/itm/1",
        source="ebay",
    )
    defaults.update(kw)
    return Listing(**defaults)
