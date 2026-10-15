#!/usr/bin/env bash
# Installe un bot Discord Python (dépôt GitHub avec bot.py + requirements.txt) comme service
# qui démarre tout seul et redémarre en cas de plantage. Pensé pour un serveur Ubuntu (Oracle Cloud).
#
#   curl -fsSLO https://raw.githubusercontent.com/Clrzz/TribeSpy/main/deploy/install.sh
#   sudo bash install.sh
#
# Le script pose ses questions une par une. On peut le relancer pour mettre à jour un bot
# ou installer un deuxième bot (chaque bot a son propre dossier, son port et ses variables).
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "Lance le script avec sudo : sudo bash install.sh"; exit 1
fi

ask() { local v; read -r -p "$1" v </dev/tty; printf '%s' "$v"; }

echo "=== Installation d'un bot Discord ==="
NAME=$(ask "Nom court du bot (ex : tribespy) : ")
NAME=$(printf '%s' "$NAME" | tr -cd 'a-zA-Z0-9_-' | tr 'A-Z' 'a-z')
[ -n "$NAME" ] || { echo "Nom vide."; exit 1; }
REPO=$(ask "Dépôt GitHub (ex : Clrzz/TribeSpy) : ")
TOKEN=$(ask "Jeton GitHub (seulement si le dépôt est privé, sinon Entrée) : ")

DIR=/opt/bots/$NAME
ENV_FILE=/etc/bots/$NAME.env
mkdir -p /opt/bots /etc/bots

echo "--> Installation de Python et Git"
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq git python3 python3-venv python3-pip >/dev/null

# Petits serveurs (1 Go, ex : VM.Standard.E2.1.Micro) : 2 Go de mémoire de secours (swap).
if [ ! -f /swapfile ] && [ "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -lt 2000000 ]; then
  echo "--> Ajout de 2 Go de mémoire de secours"
  fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap -q /swapfile && swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

id bots >/dev/null 2>&1 || useradd --system --home /opt/bots --shell /usr/sbin/nologin bots

echo "--> Téléchargement du code"
if [ -n "$TOKEN" ]; then URL="https://x-access-token:$TOKEN@github.com/$REPO.git"; else URL="https://github.com/$REPO.git"; fi
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" remote set-url origin "$URL"
  git -C "$DIR" fetch -q origin && git -C "$DIR" reset -q --hard "origin/$(git -C "$DIR" rev-parse --abbrev-ref origin/HEAD | cut -d/ -f2)"
else
  git clone -q "$URL" "$DIR"
fi
python3 -m venv "$DIR/.venv"
"$DIR/.venv/bin/pip" install -q --upgrade pip
"$DIR/.venv/bin/pip" install -q -r "$DIR/requirements.txt"

# Port local : on garde celui déjà choisi, sinon le premier libre à partir de 8001.
PORT=$(grep -s '^PORT=' "$ENV_FILE" | cut -d= -f2 || true)
if [ -z "$PORT" ]; then
  PORT=8001
  while grep -qs "^PORT=$PORT\$" /etc/bots/*.env; do PORT=$((PORT + 1)); done
fi

if [ -f "$ENV_FILE" ] && [ "$(ask "Garder les variables déjà enregistrées pour $NAME ? (o/n) : ")" = "o" ]; then
  :
else
  echo
  echo "Colle les variables du bot (une par ligne, format NOM=valeur, comme sur Render)."
  echo "Quand c'est fini, tape FIN puis Entrée."
  : > "$ENV_FILE.tmp"
  while IFS= read -r line </dev/tty; do
    line=${line%$'\r'}
    [ "$line" = "FIN" ] && break
    case "$line" in *=*) printf '%s\n' "$line" >> "$ENV_FILE.tmp" ;; esac
  done
  grep -v '^PORT=' "$ENV_FILE.tmp" > "$ENV_FILE" || true
  rm -f "$ENV_FILE.tmp"
  # Base SQLite locale si aucune base Postgres n'est fournie.
  grep -q '^SQLITE_PATH=' "$ENV_FILE" || echo "SQLITE_PATH=$DIR/data.db" >> "$ENV_FILE"
fi
sed -i '/^PORT=/d' "$ENV_FILE"; echo "PORT=$PORT" >> "$ENV_FILE"
chmod 600 "$ENV_FILE"
chown -R bots: "$DIR"

cat > "/etc/systemd/system/bot-$NAME.service" <<EOF
[Unit]
Description=Bot Discord $NAME
After=network-online.target
Wants=network-online.target

[Service]
User=bots
WorkingDirectory=$DIR
EnvironmentFile=$ENV_FILE
ExecStart=$DIR/.venv/bin/python bot.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable -q "bot-$NAME"
systemctl restart "bot-$NAME"

if [ "$(ask "Ce bot a-t-il besoin d'une adresse web https (console du site) ? (o/n) : ")" = "o" ]; then
  echo "--> Installation de Caddy (https automatique)"
  if ! DEBIAN_FRONTEND=noninteractive apt-get install -y -qq caddy >/dev/null 2>&1; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https curl gnupg >/dev/null
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg --yes
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > /etc/apt/sources.list.d/caddy-stable.list
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq caddy >/dev/null
  fi
  IP=$(curl -fsS https://api.ipify.org)
  HOST="$NAME.$(printf '%s' "$IP" | tr . -).sslip.io"
  mkdir -p /etc/caddy/sites
  grep -q 'import sites/\*' /etc/caddy/Caddyfile 2>/dev/null || echo 'import sites/*' > /etc/caddy/Caddyfile
  printf '%s {\n\treverse_proxy 127.0.0.1:%s\n}\n' "$HOST" "$PORT" > "/etc/caddy/sites/$NAME"
  # Les images Ubuntu d'Oracle bloquent tout sauf SSH : on ouvre 80 et 443.
  for p in 80 443; do
    iptables -C INPUT -p tcp --dport $p -j ACCEPT 2>/dev/null || iptables -I INPUT 1 -p tcp --dport $p -j ACCEPT
  done
  command -v netfilter-persistent >/dev/null && netfilter-persistent save >/dev/null 2>&1 || true
  systemctl reload caddy || systemctl restart caddy
  # Le bot doit connaître sa propre adresse publique (connexion Discord de la console).
  sed -i '/^PUBLIC_URL=/d' "$ENV_FILE"; echo "PUBLIC_URL=https://$HOST" >> "$ENV_FILE"
  systemctl restart "bot-$NAME"
  echo
  echo "Adresse web du bot : https://$HOST"
fi

echo
echo "✅ Bot $NAME installé et démarré."
echo "Voir ses logs : sudo journalctl -u bot-$NAME -f   (Ctrl+C pour quitter)"
