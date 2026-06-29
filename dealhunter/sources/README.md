# Sources — guide des connecteurs

Chaque source est un **module indépendant** exposant la **même interface** :

```python
class MaSource(Source):       # ou ScraperSource pour du scraping
    name = "ma_source"
    def _fetch_live(self, query: Query) -> list[Listing]: ...
```

`fetch(query) -> list[Listing]` est fourni par la classe de base : il gère le
mode fixtures, l'isolation des erreurs et le throttling. Vous n'implémentez que
`_fetch_live` (ou `_parse_html` pour un scraper).

## État des sources

| Source            | Module                  | API officielle | Activée par défaut | Note |
|-------------------|-------------------------|:--------------:|:------------------:|------|
| eBay              | `ebay.py`               | ✅ (Browse API) | ✅ | Nécessite `EBAY_CLIENT_ID/SECRET` |
| Chrono24          | `chrono24.py`           | ❌ (dealers)    | ❌ | ToS + Cloudflare |
| Bob's Watches     | `bobs_watches.py`       | ❌              | ❌ | scraping |
| Watchfinder       | `watchfinder.py`        | ❌              | ❌ | anti-bot |
| Crown & Caliber   | `crown_and_caliber.py`  | ❌              | ❌ | scraping |
| WatchBox          | `watchbox.py`           | ❌              | ❌ | scraping |
| Subdial           | `subdial.py`            | ❌              | ❌ | index prix |
| Forums (WUS…)     | `forums.py`             | ❌              | ❌ | bruité |

> ⚠️ **Légal** : seul eBay propose une API publique gratuite. Les connecteurs de
> scraping sont **désactivés par défaut**. Avant d'en activer un, vérifiez les
> Conditions d'Utilisation et le `robots.txt` de la source. Le scraping reste
> poli (rate limiting, user-agent honnête, backoff exponentiel) mais peut être
> bloqué : le système vous le signalera dans les logs (`⚠️ <source> BLOQUÉE`).

## Activer / désactiver une source

Dans `.env` :

```
ENABLED_SOURCES=ebay,bobs_watches
```

## Moteur de scraping (sources sans API)

Deux moteurs, configurables via `SCRAPER_ENGINE` dans `.env` :

| Moteur      | Récupération                              | Quand l'utiliser |
|-------------|-------------------------------------------|------------------|
| `httpx`     | GET simple + `selectolax`                 | rapide, sites peu protégés |
| `scrapling` | [Scrapling](https://github.com/D4Vinci/Scrapling) : empreinte TLS navigateur (`Fetcher`) ou navigateur furtif (`StealthyFetcher` si `SCRAPER_STEALTH=true`) | sites qui bloquent les clients basiques |

```
SCRAPER_ENGINE=scrapling
SCRAPER_STEALTH=false        # true => StealthyFetcher (anti-bot, plus lent)
```

Installation : `pip install "scrapling[fetchers]"` (le parsing marche tel quel) ;
pour le mode furtif, ajoutez **une fois** `scrapling install` (télécharge le
navigateur Camoufox). Le mode furtif change l'IP/empreinte, **pas** une garantie
contre tous les anti-bot — respectez les ToS.

**Parsing générique JSON-LD** : `ScraperSource` extrait automatiquement les
annonces depuis le balisage `schema.org` `Product`/`Offer` (présent sur beaucoup
de sites de montres) — souvent des résultats **sans écrire de sélecteurs**. Pour
un site sans JSON-LD, surchargez `_parse_html()` avec des sélecteurs CSS.

## Ajouter une nouvelle source en 3 étapes

1. **Créez le module** `dealhunter/sources/ma_source.py` :

   ```python
   from ._scraper import ScraperSource   # ou: from .base import Source

   class MaSource(ScraperSource):
       name = "ma_source"
       search_url_template = "https://exemple.com/search?q={q}"

       # Souvent rien à écrire : le parseur JSON-LD générique suffit.
       # Sinon, surchargez _parse_html() pour des sélecteurs CSS sur-mesure :
       def _parse_html(self, html, query):
           # extrayez les annonces et retournez une list[Listing]
           ...
   ```

2. **Enregistrez-la** dans `dealhunter/sources/__init__.py` :

   ```python
   from .ma_source import MaSource
   ALL_SOURCES[MaSource.name] = MaSource
   ```

3. **Activez-la** via `ENABLED_SOURCES` dans `.env`, et ajoutez éventuellement
   un fichier `fixtures/ma_source.json` pour les tests hors-ligne.

## Mode fixtures (tests / démo hors-ligne)

`USE_FIXTURES=true` fait lire chaque source dans `fixtures/<name>.json` au lieu
du réseau. Pratique pour démontrer le pipeline complet (scrape → price → score →
notify) sans clé API.
