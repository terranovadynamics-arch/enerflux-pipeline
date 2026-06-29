"""Configuration centralisée, chargée depuis l'environnement / `.env`.

Aucun secret n'est codé en dur : tout passe par des variables d'environnement
(via `.env`), conformément aux exigences de sécurité.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Tous les paramètres sont surchargables via `.env` ou l'environnement."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # --- Logique métier ------------------------------------------------------
    budget_max_usd: float = Field(50000, alias="BUDGET_MAX_USD")
    discount_threshold: float = Field(0.12, alias="DISCOUNT_THRESHOLD")
    scam_threshold: float = Field(0.45, alias="SCAM_THRESHOLD")
    price_window_days: int = Field(30, alias="PRICE_WINDOW_DAYS")
    min_comparables: int = Field(3, alias="MIN_COMPARABLES")

    # --- Boucle --------------------------------------------------------------
    poll_interval_min: int = Field(30, alias="POLL_INTERVAL_MIN")
    db_path: str = Field("dealhunter.db", alias="DB_PATH")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # --- Sources -------------------------------------------------------------
    enabled_sources: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["ebay"], alias="ENABLED_SOURCES"
    )
    use_fixtures: bool = Field(False, alias="USE_FIXTURES")
    http_user_agent: str = Field(
        "DealHunterBot/1.0", alias="HTTP_USER_AGENT"
    )
    http_rate_limit_rps: float = Field(0.5, alias="HTTP_RATE_LIMIT_RPS")

    # --- Moteur de scraping (sources sans API) ------------------------------
    # "httpx"     : GET simple + selectolax (rapide, fragile face à l'anti-bot)
    # "scrapling" : moteur Scrapling (TLS navigateur / furtif, parsing adaptatif)
    scraper_engine: str = Field("httpx", alias="SCRAPER_ENGINE")
    # True -> StealthyFetcher (vrai navigateur furtif) ; False -> Fetcher (HTTP).
    scraper_stealth: bool = Field(False, alias="SCRAPER_STEALTH")

    # --- Relais Cloudflare Worker (optionnel) -------------------------------
    # Si renseigné, toutes les requêtes des sources passent par ce relais.
    relay_url: str = Field("", alias="RELAY_URL")
    relay_secret: str = Field("", alias="RELAY_SECRET")

    # --- eBay ----------------------------------------------------------------
    ebay_client_id: str = Field("", alias="EBAY_CLIENT_ID")
    ebay_client_secret: str = Field("", alias="EBAY_CLIENT_SECRET")
    ebay_marketplace: str = Field("EBAY_US", alias="EBAY_MARKETPLACE")

    # --- Notifications -------------------------------------------------------
    notify_channels: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["email"], alias="NOTIFY_CHANNELS"
    )

    smtp_host: str = Field("", alias="SMTP_HOST")
    smtp_port: int = Field(587, alias="SMTP_PORT")
    smtp_user: str = Field("", alias="SMTP_USER")
    smtp_password: str = Field("", alias="SMTP_PASSWORD")
    smtp_from: str = Field("", alias="SMTP_FROM")
    smtp_to: Annotated[list[str], NoDecode] = Field(default_factory=list, alias="SMTP_TO")
    smtp_use_tls: bool = Field(True, alias="SMTP_USE_TLS")

    telegram_bot_token: str = Field("", alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: str = Field("", alias="TELEGRAM_CHAT_ID")

    # --- Validateurs : transforme "a,b,c" en liste --------------------------
    @field_validator(
        "enabled_sources", "notify_channels", "smtp_to", mode="before"
    )
    @classmethod
    def _split_csv(cls, v):
        if isinstance(v, str):
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    @property
    def poll_interval_seconds(self) -> int:
        return self.poll_interval_min * 60


@lru_cache
def get_settings() -> Settings:
    """Retourne l'instance unique de configuration (mémoïsée)."""
    return Settings()
