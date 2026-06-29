"""Base commune aux connecteurs de type *scraping* (sources sans API officielle).

⚠️  AVERTISSEMENT ToS / robots.txt
Aucune de ces sources n'expose d'API publique. Le scraping peut être contraire
à leurs Conditions d'Utilisation et plusieurs (Chrono24, Watchfinder...) emploient
une protection anti-bot (Cloudflare). Ces connecteurs sont donc :
    - DÉSACTIVÉS par défaut (à activer explicitement via ENABLED_SOURCES) ;
    - polis : rate limiting, user-agent honnête, backoff exponentiel ;
    - dotés d'un parseur best-effort à maintenir si le HTML du site change.

Deux moteurs de récupération (SCRAPER_ENGINE) :
    - "httpx"     : GET simple (rapide, fragile face à l'anti-bot) ;
    - "scrapling" : Scrapling (TLS navigateur / furtif), plus robuste.

Parsing : un extracteur générique **JSON-LD schema.org** (`Product`/`Offer`),
présent sur beaucoup de sites e-commerce de montres, fournit des résultats sans
sélecteurs spécifiques. Surchargez `_parse_html()` pour un parsing sur-mesure.

Pour les tests et démos, utilisez USE_FIXTURES=true (données locales).
"""
from __future__ import annotations

import json
import re

from ..models import Listing, Query, SetType
from . import _scrapling_engine
from .base import Source, SourceBlocked

# Blocs <script type="application/ld+json"> ... </script>
_JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)


class ScraperSource(Source):
    """Connecteur scraping générique : URL de recherche + parseur best-effort."""

    has_official_api = False
    enabled_by_default = False

    #: gabarit d'URL de recherche, {q} sera remplacé par les mots-clés.
    search_url_template: str = ""

    def _fetch_live(self, query: Query) -> list[Listing]:
        if not self.search_url_template:
            raise SourceBlocked(
                f"{self.name}: pas d'API et parseur non configuré — "
                "activez USE_FIXTURES=true ou implémentez _parse_html()"
            )
        self.log.warning(
            "%s: scraping actif (vérifiez le respect des ToS/robots.txt de la source)",
            self.name,
        )
        url = self.search_url_template.format(q=query.keywords.replace(" ", "+"))
        html = self._fetch_html(url)
        return self._parse(html, query)

    # ----------------------------------------------------------- récupération
    def _fetch_html(self, url: str) -> str:
        """Récupère le HTML via le moteur configuré (httpx ou scrapling)."""
        if self.settings.scraper_engine == "scrapling":
            self._throttle()
            return _scrapling_engine.fetch_page(url, self.settings)
        return self._get(url).text

    # --------------------------------------------------------------- parsing
    def _parse(self, html: str, query: Query) -> list[Listing]:
        """Pipeline de parsing : JSON-LD générique d'abord, puis hook spécifique."""
        listings = self._parse_jsonld(html, query)
        if listings:
            return listings
        return self._parse_html(html, query)

    def _parse_jsonld(self, html: str, query: Query) -> list[Listing]:
        """Extrait les annonces depuis le JSON-LD schema.org (Product/Offer).

        Générique et robuste : fonctionne sur tout site exposant des `Product`
        avec une `offers.price`. La marque/le modèle/la référence proviennent de
        la `query` en cours (on sait quoi on cherche) ; on n'extrait que le prix,
        l'URL, les images et la devise depuis l'annonce.
        """
        listings: list[Listing] = []
        for block in _JSONLD_RE.findall(html):
            try:
                data = json.loads(block.strip())
            except (json.JSONDecodeError, ValueError):
                continue
            for node in _iter_products(data):
                listing = self._product_to_listing(node, query)
                if listing is not None:
                    listings.append(listing)
        if listings:
            self.log.info("%s: %d annonce(s) via JSON-LD", self.name, len(listings))
        return listings

    def _product_to_listing(self, node: dict, query: Query) -> Listing | None:
        """Construit un Listing depuis un nœud schema.org Product."""
        offer = node.get("offers") or {}
        if isinstance(offer, list):
            offer = offer[0] if offer else {}
        price = _to_float(offer.get("price"))
        if price is None or price <= 0:
            return None
        currency = (offer.get("priceCurrency") or "USD").upper()
        if currency != "USD":
            # Pas de conversion FX intégrée : on évite de fausser les médianes.
            self.log.debug("%s: annonce ignorée (devise %s)", self.name, currency)
            return None
        url = offer.get("url") or node.get("url") or query.keywords
        images = node.get("image") or []
        if isinstance(images, str):
            images = [images]
        return Listing(
            brand=query.brand,
            model=query.model,
            reference=query.reference or _guess_ref(node.get("name", "")) or "UNKNOWN",
            asking_price_usd=price,
            currency_original=currency,
            set=SetType.UNKNOWN,
            seller=str(node.get("brand", {}).get("name", "")) or self.name,
            url=str(url),
            images=[str(i) for i in images][:4],
            source=self.name,
        )

    def _parse_html(self, html: str, query: Query) -> list[Listing]:
        """Hook de parsing sur-mesure (sélecteurs CSS par site). Vide par défaut.

        Surchargez cette méthode dans le module de la source quand le JSON-LD
        n'est pas disponible. Le HTML de chaque site étant spécifique, ce
        squelette renvoie une liste vide tant qu'il n'est pas adapté.
        """
        self.log.info(
            "%s: pas de JSON-LD exploitable et parseur CSS non spécialisé — 0 annonce.",
            self.name,
        )
        return []


# ------------------------------------------------------------------- helpers
def _iter_products(data):
    """Parcourt récursivement un document JSON-LD et yield les nœuds Product."""
    if isinstance(data, list):
        for item in data:
            yield from _iter_products(item)
    elif isinstance(data, dict):
        if "@graph" in data:
            yield from _iter_products(data["@graph"])
        t = data.get("@type", "")
        types = t if isinstance(t, list) else [t]
        if any(str(x).lower() == "product" for x in types) and "offers" in data:
            yield data


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return None


_REF_RE = re.compile(r"\b([0-9]{4,6}[A-Z]{0,4})\b")


def _guess_ref(name: str) -> str | None:
    """Devine une référence depuis un titre (repli quand la query n'en a pas)."""
    m = _REF_RE.search(name or "")
    return m.group(1) if m else None
