"""Modèle de données normalisé pour une annonce + structures associées."""
from __future__ import annotations

import enum
import hashlib
from datetime import datetime, timezone

from pydantic import BaseModel, Field, field_validator


class SetType(str, enum.Enum):
    """Complétude de l'ensemble vendu."""

    FULL = "full"            # boîte + papiers + montre
    PAPERS_ONLY = "papers-only"
    WATCH_ONLY = "watch-only"
    UNKNOWN = "unknown"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Listing(BaseModel):
    """Annonce normalisée (sortie commune de toutes les sources)."""

    brand: str
    model: str
    reference: str
    year: int | None = None
    condition: str | None = None            # ex: "new", "very good", "unworn"...
    set: SetType = SetType.UNKNOWN
    asking_price_usd: float
    currency_original: str = "USD"
    seller: str | None = None
    seller_feedback: float | None = None     # score/feedback vendeur (0-100 ou note)
    seller_sales_count: int | None = None
    location: str | None = None
    url: str
    images: list[str] = Field(default_factory=list)
    source: str = "unknown"                  # nom de la source d'origine
    scraped_at: datetime = Field(default_factory=utcnow)

    @field_validator("reference")
    @classmethod
    def _normalize_ref(cls, v: str) -> str:
        # Normalise la référence : majuscules, sans espaces parasites.
        return v.strip().upper()

    @property
    def fingerprint(self) -> str:
        """Hash de dédoublonnage = url + référence + prix (cf. cahier des charges).

        Deux annonces partageant url+réf+prix sont considérées identiques.
        """
        raw = f"{self.url}|{self.reference}|{round(self.asking_price_usd)}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @property
    def set_class(self) -> str:
        """Classe de set utilisée pour grouper les comparables."""
        return self.set.value


class Query(BaseModel):
    """Requête de recherche envoyée à une source."""

    brand: str
    model: str
    reference: str | None = None
    keywords: str = ""           # texte libre construit pour la recherche
    max_price_usd: float = 50000


class Deal(BaseModel):
    """Résultat de scoring : une annonce jugée intéressante (ou suspecte)."""

    listing: Listing
    reference_price_usd: float
    deal_score: float            # (ref - asking) / ref
    is_alert: bool
    is_suspicious: bool          # décote anormale -> possible arnaque
    seller_ok: bool
    reason: str                  # phrase "pourquoi c'est une affaire"
    comparables_count: int

    @property
    def discount_pct(self) -> float:
        return round(self.deal_score * 100, 1)
