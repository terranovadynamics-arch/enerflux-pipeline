# Relais de fetch Cloudflare Worker

Permet aux connecteurs Deal Hunter d'exécuter leurs requêtes **depuis l'edge
Cloudflare** au lieu de l'IP de votre serveur. Utile quand une source bloque les
IP de datacenter/VPS, pour centraliser le rate-limiting et masquer l'origine.

> ⚠️ Le relais ne contourne **pas** les protections anti-bot avancées (Cloudflare
> côté cible, captcha). Il change seulement l'IP source. Respectez les ToS.

## Déploiement

Prérequis : un compte Cloudflare + [wrangler](https://developers.cloudflare.com/workers/wrangler/).

```bash
cd deploy/cloudflare-relay
npm install -g wrangler        # si besoin
wrangler login

# Secret partagé (généré aléatoirement, gardé hors du code) :
openssl rand -hex 24           # copiez la valeur
wrangler secret put RELAY_SECRET   # collez-la quand demandé

wrangler deploy
# -> URL du type https://dealhunter-relay.<votre-sous-domaine>.workers.dev
```

Ajustez si besoin l'allowlist d'hôtes dans `wrangler.toml` (`ALLOWED_HOSTS`).

## Branchement côté agent

Dans le `.env` de Deal Hunter :

```
RELAY_URL=https://dealhunter-relay.<votre-sous-domaine>.workers.dev
RELAY_SECRET=<le même secret que ci-dessus>
```

Quand `RELAY_URL` est défini, **toutes** les requêtes des sources (eBay inclus)
passent par le relais. Laissez vide pour un accès direct.

## Contrat de l'API du relais

```
GET|POST  {RELAY_URL}/fetch?url=<URL cible URL-encodée>
Header    X-Relay-Secret: <secret>
```

Le Worker rejoue méthode + corps + headers vers la cible (après vérification du
secret et de l'allowlist) et renvoie la réponse upstream. `GET /` renvoie un
message de santé.

## Test rapide

```bash
curl -H "X-Relay-Secret: $RELAY_SECRET" \
  "$RELAY_URL/fetch?url=https%3A%2F%2Fapi.ebay.com%2F"
```
