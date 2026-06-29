"""Interface commune des connecteurs de source.

Un module = une source. Toutes implémentent la même interface :
    fetch(query) -> list[Listing]
Cela permet d'ajouter/retirer une source sans toucher au reste du système.

Bonnes pratiques imposées par le cahier des charges :
    - respecter robots.txt / ToS : APIs officielles si dispo, sinon scraping poli ;
    - rate limiting + user-agent honnête + backoff exponentiel.
"""
from __future__ import annotations

import abc
import json
import time
from pathlib import Path

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..config import get_settings
from ..logging_conf import get_logger
from ..models import Listing, Query
from .. import transport

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class SourceError(Exception):
    """Erreur récupérable côté source (réseau, parsing, blocage)."""


class SourceBlocked(SourceError):
    """La source bloque l'accès (Cloudflare, 403, captcha) ou exige une clé API."""


class Source(abc.ABC):
    """Connecteur de source abstrait."""

    #: identifiant unique (clé d'enregistrement et nom de fixture)
    name: str = "base"
    #: True si la source a une API officielle (pas de scraping)
    has_official_api: bool = False
    #: True si activée par défaut (les scrapers ToS-sensibles = False)
    enabled_by_default: bool = False

    def __init__(self) -> None:
        self.settings = get_settings()
        self.log = get_logger(f"source.{self.name}")
        self._last_request_ts = 0.0

    # ------------------------------------------------------------------ public
    def fetch(self, query: Query) -> list[Listing]:
        """Point d'entrée : retourne des annonces normalisées.

        En mode fixtures (USE_FIXTURES=true), renvoie les données locales —
        utile pour les tests et les démos hors-ligne.
        """
        if self.settings.use_fixtures:
            return self._load_fixtures(query)
        try:
            return self._fetch_live(query)
        except SourceBlocked:
            raise
        except SourceError:
            raise
        except Exception as exc:  # pragma: no cover - filet de sécurité
            raise SourceError(str(exc)) from exc

    # --------------------------------------------------------------- à override
    @abc.abstractmethod
    def _fetch_live(self, query: Query) -> list[Listing]:
        """Implémentation réelle (API ou scraping)."""

    # ----------------------------------------------------------------- helpers
    def _throttle(self) -> None:
        """Rate limiting poli : respecte HTTP_RATE_LIMIT_RPS."""
        rps = max(self.settings.http_rate_limit_rps, 0.01)
        min_interval = 1.0 / rps
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_ts = time.monotonic()

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        wait=wait_exponential(multiplier=2, min=2, max=32),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _get(self, url: str, **kwargs) -> httpx.Response:
        """GET (direct ou via relais Cloudflare) avec UA honnête, throttling, backoff."""
        self._throttle()
        headers = kwargs.pop("headers", {})
        headers.setdefault("User-Agent", self.settings.http_user_agent)
        params = kwargs.pop("params", None)
        resp = transport.request("GET", url, headers=headers, params=params, **kwargs)
        if resp.status_code in (401, 403, 429) or resp.status_code == 503:
            raise SourceBlocked(
                f"{self.name}: HTTP {resp.status_code} — source bloquée ou quota dépassé"
            )
        resp.raise_for_status()
        return resp

    def _load_fixtures(self, query: Query) -> list[Listing]:
        """Charge les annonces d'exemple depuis fixtures/<name>.json."""
        path = FIXTURES_DIR / f"{self.name}.json"
        if not path.exists():
            return []
        raw = json.loads(path.read_text(encoding="utf-8"))
        listings = [Listing(**item) for item in raw]
        return [self._match(query, ls) for ls in listings if self._match(query, ls)]

    @staticmethod
    def _match(query: Query, listing: Listing) -> Listing | None:
        """Filtre une annonce selon la requête (référence si fournie)."""
        if query.reference and query.reference.upper() not in listing.reference.upper():
            return None
        if listing.asking_price_usd > query.max_price_usd:
            return None
        return listing
