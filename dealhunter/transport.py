"""Couche transport HTTP : accès direct OU via un relais Cloudflare Worker.

Si RELAY_URL est configuré (.env), toutes les requêtes des connecteurs sont
routées à travers un Worker Cloudflare qui les exécute depuis l'edge Cloudflare
(IP non-datacenter, rate-limiting centralisé, origine masquée). Sinon, accès
direct via httpx.

Contrat du relais :
    GET/POST  {RELAY_URL}/fetch?url=<URL cible encodée>
    Header    X-Relay-Secret: <secret partagé>
    -> le Worker rejoue la méthode/corps/headers vers la cible et renvoie la
       réponse upstream telle quelle.
"""
from __future__ import annotations

import urllib.parse

import httpx

from .config import get_settings


def target_url(url: str, params: dict | None) -> str:
    """Construit l'URL cible complète (avec querystring) — testable sans réseau."""
    if not params:
        return url
    return str(httpx.URL(url, params=params))


def relay_endpoint(relay_url: str, full_target: str) -> str:
    """Construit l'URL du relais encapsulant la cible — testable sans réseau."""
    encoded = urllib.parse.quote(full_target, safe="")
    return f"{relay_url.rstrip('/')}/fetch?url={encoded}"


def request(
    method: str,
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    content=None,
    data=None,
    timeout: float = 20.0,
) -> httpx.Response:
    """Exécute une requête HTTP, directement ou via le relais Cloudflare."""
    settings = get_settings()
    headers = dict(headers or {})
    full = target_url(url, params)

    if settings.relay_url:
        endpoint = relay_endpoint(settings.relay_url, full)
        if settings.relay_secret:
            headers["X-Relay-Secret"] = settings.relay_secret
        return httpx.request(
            method, endpoint, headers=headers, content=content, data=data, timeout=timeout
        )

    # Accès direct (les params sont déjà inclus dans `full`).
    return httpx.request(
        method, full, headers=headers, content=content, data=data, timeout=timeout
    )
