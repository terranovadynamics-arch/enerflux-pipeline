"""Connecteur eBay — la SEULE source avec une API officielle gratuite.

Utilise la Browse API (https://developer.ebay.com/api-docs/buy/browse/overview.html)
avec le flux OAuth "client credentials". Nécessite EBAY_CLIENT_ID / EBAY_CLIENT_SECRET
dans le `.env` (créez une app gratuite sur le portail développeur eBay).

Catégorie montres-bracelets : 31387. On filtre prix <= budget, devise USD.
"""
from __future__ import annotations

import base64
import time

import httpx

from ..models import Listing, Query, SetType
from .. import transport
from .base import Source, SourceBlocked, SourceError

OAUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
BROWSE_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
WATCH_CATEGORY = "31387"  # Wristwatches


class EbaySource(Source):
    name = "ebay"
    has_official_api = True
    enabled_by_default = True

    def __init__(self) -> None:
        super().__init__()
        self._token: str | None = None
        self._token_expiry: float = 0.0

    # ----------------------------------------------------------------- OAuth
    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        if not (self.settings.ebay_client_id and self.settings.ebay_client_secret):
            raise SourceBlocked(
                "eBay: EBAY_CLIENT_ID/SECRET manquants — créez une clé API gratuite "
                "sur https://developer.ebay.com/ et renseignez le .env"
            )
        creds = f"{self.settings.ebay_client_id}:{self.settings.ebay_client_secret}"
        auth = base64.b64encode(creds.encode()).decode()
        self._throttle()
        resp = transport.request(
            "POST",
            OAUTH_URL,
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "client_credentials",
                "scope": "https://api.ebay.com/oauth/api_scope",
            },
            timeout=20.0,
        )
        if resp.status_code != 200:
            raise SourceBlocked(f"eBay OAuth a échoué: HTTP {resp.status_code}")
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expiry = time.time() + int(payload.get("expires_in", 7200))
        return self._token

    # ------------------------------------------------------------------ fetch
    def _fetch_live(self, query: Query) -> list[Listing]:
        token = self._get_token()
        params = {
            "q": query.keywords,
            "category_ids": WATCH_CATEGORY,
            "filter": (
                f"price:[..{int(query.max_price_usd)}],"
                "priceCurrency:USD,"
                "buyingOptions:{FIXED_PRICE|AUCTION}"
            ),
            "limit": "50",
            "sort": "price",
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": self.settings.ebay_marketplace,
        }
        self._throttle()
        try:
            resp = transport.request(
                "GET", BROWSE_URL, params=params, headers=headers, timeout=20.0
            )
        except httpx.HTTPError as exc:
            raise SourceError(f"eBay: erreur réseau {exc}") from exc
        if resp.status_code in (401, 403, 429):
            raise SourceBlocked(f"eBay: HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise SourceError(f"eBay: HTTP {resp.status_code}")

        items = resp.json().get("itemSummaries", []) or []
        listings: list[Listing] = []
        for it in items:
            listing = self._to_listing(it, query)
            if listing:
                listings.append(listing)
        return listings

    # --------------------------------------------------------------- mapping
    def _to_listing(self, it: dict, query: Query) -> Listing | None:
        price = it.get("price", {})
        if price.get("currency") != "USD":
            return None
        try:
            amount = float(price["value"])
        except (KeyError, TypeError, ValueError):
            return None

        seller = it.get("seller", {}) or {}
        images = []
        if it.get("image", {}).get("imageUrl"):
            images.append(it["image"]["imageUrl"])
        images += [
            img["imageUrl"] for img in it.get("additionalImages", []) if img.get("imageUrl")
        ]

        return Listing(
            brand=query.brand,
            model=query.model,
            reference=query.reference or query.model,
            condition=it.get("condition"),
            set=SetType.UNKNOWN,
            asking_price_usd=amount,
            currency_original="USD",
            seller=seller.get("username"),
            seller_feedback=_to_float(seller.get("feedbackPercentage")),
            seller_sales_count=_to_int(seller.get("feedbackScore")),
            location=(it.get("itemLocation", {}) or {}).get("country"),
            url=it.get("itemWebUrl") or it.get("itemHref", ""),
            images=images[:2],
            source=self.name,
        )


def _to_float(v) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
