# 🕵️ Deal Hunter — agent autonome montres de luxe

Surveille en continu le marché des montres de luxe d'occasion (Rolex, Audemars
Piguet, Patek Philippe) et **alerte uniquement** sur les vraies bonnes affaires
**sous 50 000 USD**.

> Ce module est autonome et vit dans le dossier `dealhunter/`, à côté de l'ancien
> pipeline `enerflux`. Il ne modifie pas ce dernier.

## Architecture (agent principal + sous-agents)

```
orchestrator ── boucle de surveillance, dédoublonnage, scoring final
  ├── scraper   (1 instance par source)   → récupère les annonces
  ├── pricer                              → prix de référence marché (médiane / réf)
  ├── scorer                              → deal_score + décision d'alerte
  └── notifier                            → email / Telegram (digest)
```

État persistant en **SQLite** (`dealhunter.db`) : annonces vues, alertes
envoyées, historique de prix par référence → **reprise propre après crash**.

## Logique "bonne affaire"

1. `pricer` calcule un **prix de référence = médiane** des annonces comparables
   (même réf + set similaire) sur une **fenêtre glissante de 30 jours**, toutes
   sources confondues. L'historique est conservé pour suivre la tendance.
2. `deal_score = (prix_référence − asking_price) / prix_référence`.
3. **ALERTE** si **toutes** ces conditions sont vraies :
   - `asking_price_usd ≤ BUDGET_MAX_USD` (50 000)
   - `deal_score ≥ DISCOUNT_THRESHOLD` (12 %, configurable)
   - vendeur crédible (feedback OK, pas de red flag évident)
   - annonce **jamais alertée** (dédoublonnage par hash `url + réf + prix`)
4. **Anti-arnaque** : une décote `> SCAM_THRESHOLD` (45 %) n'est **pas** validée
   automatiquement → signalée avec le flag **« À VÉRIFIER, possible arnaque »**.

## Sources

Seul **eBay** dispose d'une API officielle gratuite (et est activé par défaut).
Les autres connecteurs (Chrono24, Bob's Watches, Watchfinder, Crown & Caliber,
WatchBox, Subdial, forums) sont implémentés derrière la même interface mais
**désactivés par défaut** (scraping ToS-sensible). Détails et guide d'ajout :
[`dealhunter/sources/README.md`](dealhunter/sources/README.md).

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dealhunter.txt
cp .env.example .env          # puis éditez vos valeurs
```

### Clé API eBay (gratuite)

1. Créez un compte sur https://developer.ebay.com/
2. Créez une application **Production**, récupérez `App ID (Client ID)` et
   `Cert ID (Client Secret)`.
3. Renseignez `EBAY_CLIENT_ID` et `EBAY_CLIENT_SECRET` dans `.env`.

### Notifications (email)

Renseignez `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`,
`SMTP_TO` dans `.env`. (Telegram est disponible en option : `TELEGRAM_BOT_TOKEN`,
`TELEGRAM_CHAT_ID`, puis ajoutez `telegram` à `NOTIFY_CHANNELS`.)

## Lancement

```bash
# Démarrage unique (mode service "non-stop")
python -m dealhunter

# Un seul cycle (pour cron ou test)
python -m dealhunter --once

# Vérifier la config et les sources actives
python -m dealhunter --check
```

### Démo hors-ligne (sans clé API)

```bash
USE_FIXTURES=true python -m dealhunter --once
```

Utilise des annonces d'exemple (`dealhunter/sources/fixtures/`) pour démontrer le
pipeline complet : il doit détecter une bonne affaire sur la Submariner et une
annonce suspecte (décote anormale).

## Exécution en arrière-plan (service)

- **systemd** : voir [`deploy/dealhunter.service`](deploy/dealhunter.service)
- **cron** : voir [`deploy/crontab.example`](deploy/crontab.example)

## Configuration (`.env`)

| Variable | Défaut | Rôle |
|---|---|---|
| `BUDGET_MAX_USD` | 50000 | Plafond de prix |
| `DISCOUNT_THRESHOLD` | 0.12 | Décote minimale pour alerter |
| `SCAM_THRESHOLD` | 0.45 | Au-delà → flag "possible arnaque" |
| `POLL_INTERVAL_MIN` | 30 | Intervalle de la boucle |
| `PRICE_WINDOW_DAYS` | 30 | Fenêtre de la médiane |
| `MIN_COMPARABLES` | 3 | Comparables min pour fiabiliser |
| `ENABLED_SOURCES` | ebay | Sources actives |
| `USE_FIXTURES` | false | Mode démo hors-ligne |
| `NOTIFY_CHANNELS` | email | Canaux d'alerte |

> 🔒 **Aucun secret n'est codé en dur** : tout passe par `.env` (jamais commité).

## Tests

```bash
pip install -r requirements-dealhunter.txt
python -m pytest tests/ -v
```

Couvre le **scorer** (décote, seuils, anti-arnaque, vendeur), le **pricer**
(médiane, fenêtre, repli de set) et le **dédoublonnage** (empreinte url+réf+prix).

## Modèle de données

`brand, model, reference, year, condition, set (full/papers-only/watch-only),
asking_price_usd, currency_original, seller, location, url, images[], scraped_at`
(voir `dealhunter/models.py`).
