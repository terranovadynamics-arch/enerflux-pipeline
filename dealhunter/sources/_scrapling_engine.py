"""Adaptateur Scrapling : récupération de page robuste pour les sources scraping.

Scrapling (https://github.com/D4Vinci/Scrapling) fournit :
    - Fetcher        : HTTP avec empreinte TLS de navigateur (rapide) ;
    - StealthyFetcher: vrai navigateur furtif (contourne plus d'anti-bot, lent).

L'import est paresseux : Scrapling reste une dépendance optionnelle. Si elle
n'est pas installée, on lève une erreur explicite plutôt que de planter à
l'import du module.
"""
from __future__ import annotations

from ..config import Settings
from .base import SourceBlocked


def _import_scrapling():
    try:
        from scrapling.fetchers import Fetcher, StealthyFetcher  # type: ignore
    except ImportError as exc:  # pragma: no cover - dépend de l'install
        raise SourceBlocked(
            "Moteur 'scrapling' demandé mais le paquet n'est pas installé. "
            "Installez-le : pip install \"scrapling[fetchers]\" "
            "(puis `scrapling install` pour le mode furtif)."
        ) from exc
    return Fetcher, StealthyFetcher


def fetch_page(url: str, settings: Settings) -> str:
    """Récupère une page via Scrapling et renvoie son HTML brut.

    Choisit le fetcher selon SCRAPER_STEALTH. Lève SourceBlocked si la page est
    bloquée (statut 401/403/429/503) ou en cas d'erreur réseau.
    """
    Fetcher, StealthyFetcher = _import_scrapling()
    headers = {"User-Agent": settings.http_user_agent}
    try:
        if settings.scraper_stealth:
            # Navigateur furtif : headers gérés par le moteur, humanize=anti-bot.
            page = StealthyFetcher.fetch(url, headless=True, humanize=True)
        else:
            page = Fetcher.get(url, headers=headers, stealthy_headers=True)
    except Exception as exc:  # réseau, timeout, navigateur manquant...
        raise SourceBlocked(f"scrapling: échec de récupération ({exc})") from exc

    status = getattr(page, "status", 200)
    if status in (401, 403, 429, 503):
        raise SourceBlocked(f"scrapling: HTTP {status} — source bloquée ou quota")
    return page.html_content
