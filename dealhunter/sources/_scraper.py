"""Base commune aux connecteurs de type *scraping* (sources sans API officielle).

⚠️  AVERTISSEMENT ToS / robots.txt
Aucune de ces sources n'expose d'API publique. Le scraping peut être contraire
à leurs Conditions d'Utilisation et plusieurs (Chrono24, Watchfinder...) emploient
une protection anti-bot (Cloudflare). Ces connecteurs sont donc :
    - DÉSACTIVÉS par défaut (à activer explicitement via ENABLED_SOURCES) ;
    - polis : rate limiting, user-agent honnête, backoff exponentiel ;
    - dotés d'un parseur best-effort à maintenir si le HTML du site change.

Pour les tests et démos, utilisez USE_FIXTURES=true (données locales).
"""
from __future__ import annotations

from selectolax.parser import HTMLParser

from ..models import Listing, Query
from .base import Source, SourceBlocked


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
        resp = self._get(url)
        return self._parse_html(resp.text, query)

    def _parse_html(self, html: str, query: Query) -> list[Listing]:
        """À spécialiser par site. Par défaut : best-effort générique (souvent vide).

        Surchargez cette méthode dans le module de la source pour extraire les
        annonces réelles. Le HTML de chaque site étant spécifique, ce squelette
        renvoie une liste vide tant qu'il n'est pas adapté.
        """
        _ = HTMLParser(html)  # point d'ancrage pour l'implémentation par site
        self.log.info(
            "%s: parseur HTML non spécialisé — 0 annonce. "
            "Adaptez _parse_html() pour cette source.",
            self.name,
        )
        return []
