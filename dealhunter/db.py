"""Persistance SQLite (SQLAlchemy) : annonces vues, alertes envoyées, historique prix.

Permet une reprise propre après crash/redémarrage : tout l'état est sur disque.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import (
    Float,
    Integer,
    String,
    DateTime,
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from .models import Listing


class Base(DeclarativeBase):
    pass


class SeenListing(Base):
    """Annonce déjà observée (dédoublonnage par empreinte url+réf+prix)."""

    __tablename__ = "seen_listings"

    fingerprint: Mapped[str] = mapped_column(String, primary_key=True)
    url: Mapped[str] = mapped_column(String)
    reference: Mapped[str] = mapped_column(String, index=True)
    asking_price_usd: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String)
    first_seen: Mapped[datetime] = mapped_column(DateTime)


class SentAlert(Base):
    """Alerte déjà envoyée (évite de réalerter sur la même affaire)."""

    __tablename__ = "sent_alerts"

    fingerprint: Mapped[str] = mapped_column(String, primary_key=True)
    reference: Mapped[str] = mapped_column(String, index=True)
    asking_price_usd: Mapped[float] = mapped_column(Float)
    deal_score: Mapped[float] = mapped_column(Float)
    url: Mapped[str] = mapped_column(String)
    sent_at: Mapped[datetime] = mapped_column(DateTime)


class PricePoint(Base):
    """Historique de prix par référence (pour la médiane + la tendance)."""

    __tablename__ = "price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reference: Mapped[str] = mapped_column(String, index=True)
    set_class: Mapped[str] = mapped_column(String, index=True)
    condition: Mapped[str | None] = mapped_column(String, nullable=True)
    asking_price_usd: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String)
    fingerprint: Mapped[str] = mapped_column(String, index=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, index=True)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    """Façade simple au-dessus de SQLAlchemy."""

    def __init__(self, db_path: str):
        self.engine = create_engine(f"sqlite:///{db_path}", future=True)
        Base.metadata.create_all(self.engine)

    # --- Dédoublonnage -------------------------------------------------------
    def is_seen(self, fingerprint: str) -> bool:
        with Session(self.engine) as s:
            return s.get(SeenListing, fingerprint) is not None

    def mark_seen(self, listing: Listing) -> bool:
        """Marque une annonce comme vue. Retourne True si elle est nouvelle."""
        with Session(self.engine) as s:
            if s.get(SeenListing, listing.fingerprint) is not None:
                return False
            s.add(
                SeenListing(
                    fingerprint=listing.fingerprint,
                    url=listing.url,
                    reference=listing.reference,
                    asking_price_usd=listing.asking_price_usd,
                    source=listing.source,
                    first_seen=_utcnow(),
                )
            )
            s.commit()
            return True

    def is_alerted(self, fingerprint: str) -> bool:
        with Session(self.engine) as s:
            return s.get(SentAlert, fingerprint) is not None

    def mark_alerted(self, listing: Listing, deal_score: float) -> None:
        with Session(self.engine) as s:
            if s.get(SentAlert, listing.fingerprint) is not None:
                return
            s.add(
                SentAlert(
                    fingerprint=listing.fingerprint,
                    reference=listing.reference,
                    asking_price_usd=listing.asking_price_usd,
                    deal_score=deal_score,
                    url=listing.url,
                    sent_at=_utcnow(),
                )
            )
            s.commit()

    # --- Historique de prix --------------------------------------------------
    def record_price(self, listing: Listing) -> None:
        """Enregistre un point de prix (idempotent par empreinte + jour)."""
        with Session(self.engine) as s:
            s.add(
                PricePoint(
                    reference=listing.reference,
                    set_class=listing.set_class,
                    condition=listing.condition,
                    asking_price_usd=listing.asking_price_usd,
                    source=listing.source,
                    fingerprint=listing.fingerprint,
                    observed_at=listing.scraped_at,
                )
            )
            s.commit()

    def prices_for(
        self, reference: str, set_class: str | None, window_days: int
    ) -> list[float]:
        """Prix comparables actifs sur la fenêtre glissante (pour la médiane)."""
        since = _utcnow() - timedelta(days=window_days)
        stmt = select(PricePoint.asking_price_usd).where(
            PricePoint.reference == reference,
            PricePoint.observed_at >= since,
        )
        if set_class is not None:
            stmt = stmt.where(PricePoint.set_class == set_class)
        with Session(self.engine) as s:
            return [row[0] for row in s.execute(stmt).all()]

    def price_trend(
        self, reference: str, set_class: str | None, window_days: int
    ) -> list[tuple[datetime, float]]:
        """Série (date, prix) pour suivre la tendance d'une référence."""
        since = _utcnow() - timedelta(days=window_days)
        stmt = select(PricePoint.observed_at, PricePoint.asking_price_usd).where(
            PricePoint.reference == reference,
            PricePoint.observed_at >= since,
        )
        if set_class is not None:
            stmt = stmt.where(PricePoint.set_class == set_class)
        stmt = stmt.order_by(PricePoint.observed_at)
        with Session(self.engine) as s:
            return [(row[0], row[1]) for row in s.execute(stmt).all()]
