#!/usr/bin/env bash
# ============================================================================
#  Deal Hunter — installation sur un VPS (Debian/Ubuntu)
#  Idempotent : peut être relancé sans danger.
#
#  Usage :
#     sudo bash deploy/install_vps.sh          # depuis un repo déjà cloné
#   ou, en une commande sur un VPS vierge :
#     curl -fsSL <RAW_URL>/deploy/install_vps.sh | sudo bash
# ============================================================================
set -euo pipefail

# --- Paramètres (surchargeables par variables d'env) ------------------------
APP_DIR="${APP_DIR:-/opt/dealhunter}"
APP_USER="${APP_USER:-dealhunter}"
REPO_URL="${REPO_URL:-https://github.com/terranovadynamics-arch/enerflux-pipeline.git}"
BRANCH="${BRANCH:-claude/deal-hunter-watch-agent-x3kt13}"

echo "==> Deal Hunter — installation dans ${APP_DIR} (branche ${BRANCH})"

# --- Dépendances système ----------------------------------------------------
if command -v apt-get >/dev/null 2>&1; then
  apt-get update -y
  apt-get install -y python3 python3-venv python3-pip git
fi

# --- Utilisateur de service dédié (sans shell) ------------------------------
if ! id "${APP_USER}" >/dev/null 2>&1; then
  echo "==> Création de l'utilisateur de service ${APP_USER}"
  useradd --system --create-home --shell /usr/sbin/nologin "${APP_USER}"
fi

# --- Récupération / mise à jour du code -------------------------------------
if [ -d "${APP_DIR}/.git" ]; then
  echo "==> Mise à jour du dépôt existant"
  git -C "${APP_DIR}" fetch origin "${BRANCH}"
  git -C "${APP_DIR}" checkout "${BRANCH}"
  git -C "${APP_DIR}" pull origin "${BRANCH}"
else
  echo "==> Clonage du dépôt"
  git clone --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}"
fi

# --- Environnement virtuel + dépendances ------------------------------------
echo "==> Création de l'environnement Python"
python3 -m venv "${APP_DIR}/.venv"
"${APP_DIR}/.venv/bin/pip" install --upgrade pip
"${APP_DIR}/.venv/bin/pip" install -r "${APP_DIR}/requirements-dealhunter.txt"

# --- Fichier .env (à compléter par l'utilisateur) ---------------------------
if [ ! -f "${APP_DIR}/.env" ]; then
  echo "==> Création de ${APP_DIR}/.env depuis le modèle (À COMPLÉTER)"
  cp "${APP_DIR}/.env.example" "${APP_DIR}/.env"
  chmod 600 "${APP_DIR}/.env"
fi

# --- Droits -----------------------------------------------------------------
chown -R "${APP_USER}:${APP_USER}" "${APP_DIR}"

# --- Service systemd --------------------------------------------------------
echo "==> Installation du service systemd"
install -m 644 "${APP_DIR}/deploy/dealhunter.service" /etc/systemd/system/dealhunter.service
systemctl daemon-reload

cat <<EOF

============================================================================
 Installation terminée.

 1) Éditez vos secrets (clé eBay, SMTP) :
        sudo nano ${APP_DIR}/.env

 2) Vérifiez la config :
        sudo -u ${APP_USER} ${APP_DIR}/.venv/bin/python -m dealhunter --check

 3) Test d'un cycle unique (affiche les affaires) :
        cd ${APP_DIR} && sudo -u ${APP_USER} ${APP_DIR}/.venv/bin/python -m dealhunter --once

 4) Démarrez le service "non-stop" :
        sudo systemctl enable --now dealhunter
        journalctl -u dealhunter -f      # suivre les logs en direct
============================================================================
EOF
