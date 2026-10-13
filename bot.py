"""
TribeSpy — public (multi-server) Discord bot tracking Steam A2S compatible game servers.
English by default, French optional per Discord server (/config language).

* Chaque serveur Discord a sa propre configuration, ses propres serveurs de jeu et ses propres données.
* Aucun intent privilégié requis (pas de "message content").
* Lancement : python bot.py   (variables d'environnement : voir README.md)
"""
import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import socket
import sqlite3
import time
from urllib.parse import urlencode, urlsplit
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, available_timezones

import a2s
import aiohttp
import discord
from aiohttp import web
from discord import app_commands
from discord.ext import commands, tasks

try:
    import psycopg2
    from psycopg2 import pool as pg_pool
    from psycopg2.extras import execute_batch
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tribespy")

# ======================================================================================
# CONFIGURATION
# ======================================================================================
BRAND = os.environ.get("BOT_BRAND", "TribeSpy")
TOKEN = os.environ.get("DISCORD_TOKEN")
SITE_URL = os.environ.get("SITE_URL", "")
SUPPORT_URL = os.environ.get("SUPPORT_URL", "")
PREMIUM_URL = os.environ.get("PREMIUM_URL", SITE_URL)
DEFAULT_TZ = os.environ.get("DEFAULT_TZ", "Europe/Paris")

TICK = 60                                                        # secondes entre deux relevés
FAIL_THRESHOLD = 2                                               # échecs consécutifs avant "Hors ligne"
INACTIVE_DAYS = int(os.environ.get("ROSTER_INACTIVE_DAYS", "30"))  # fiche joueurs : retrait après N jours
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "180"))      # purge auto de l'historique / temps de jeu
LEFT_GRACE_DAYS = 30                                             # données conservées N jours après expulsion du bot
MAX_LIST = 500                                                   # taille max blacklist / tribus par serveur Discord

# Offres : modifie librement ces valeurs pour ajuster gratuit / premium.
# FREE_MODE=1 (par défaut) : tout est gratuit, toutes les fonctions sont ouvertes à tous les serveurs.
# FREE_MODE=0 : réactive les offres gratuit / premium ci-dessous.
FREE_MODE = os.environ.get("FREE_MODE", "1") != "0"
PLANS = {
    "free": {"label": "Free", "max_servers": int(os.environ.get("FREE_MAX_SERVERS", "10" if FREE_MODE else "3")),
             "max_blacklist": MAX_LIST if FREE_MODE else 10,
             "roster": FREE_MODE, "summaries": FREE_MODE, "raid": FREE_MODE},
    "premium": {"label": "Premium", "max_servers": int(os.environ.get("PREMIUM_MAX_SERVERS", "30")),
                "max_blacklist": MAX_LIST, "roster": True, "summaries": True, "raid": True},
}


def feat(g, key):
    return PLANS.get(g.get("plan") or "free", PLANS["free"])[key]


DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
IS_PG = bool(DATABASE_URL and HAS_PSYCOPG2)
DB_NAME = os.environ.get("SQLITE_PATH", "tribespy.db")
# Schéma PostgreSQL dédié : permet de partager une base (ex. Supabase) avec un autre projet sans collision de tables.
DB_SCHEMA = os.environ.get("DB_SCHEMA", "tribespy")
if not re.match(r"^[a-z_][a-z0-9_]{0,40}$", DB_SCHEMA):
    raise SystemExit("DB_SCHEMA invalide : lettres minuscules, chiffres et _ uniquement.")
if IS_PG and ":6543" in DATABASE_URL:
    raise SystemExit("DATABASE_URL utilise le « Transaction pooler » (port 6543). Utilise le « Session pooler » (port 5432).")

NO_MENTIONS = discord.AllowedMentions.none()
PING_HERE = discord.AllowedMentions(everyone=True)


def utcnow():
    return datetime.now(timezone.utc)


# ======================================================================================
# TEXTES (anglais par défaut, français en option : /config language)
# ======================================================================================
LANGS = {"en": "English", "fr": "Français"}
DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")
if DEFAULT_LANG not in LANGS:
    DEFAULT_LANG = "en"

STR = {
    # statuts / offres
    "st_online": {"en": "Online", "fr": "En ligne"},
    "st_offline": {"en": "Offline", "fr": "Hors ligne"},
    "st_unknown": {"en": "Unknown", "fr": "Inconnu"},
    "plan_free": {"en": "Free", "fr": "Gratuit"},
    "plan_premium": {"en": "Premium", "fr": "Premium"},
    # durées
    "ago_now": {"en": "just now", "fr": "à l'instant"},
    "ago_min": {"en": "{n} min ago", "fr": "il y a {n} min"},
    "ago_h": {"en": "{h}h{m:02d} ago", "fr": "il y a {h}h{m:02d}"},
    "ago_d": {"en": "{n} d ago", "fr": "il y a {n} j"},
    "pack_more": {"en": "… and {n} more — refine your search", "fr": "… et {n} autre(s) — précise ta recherche"},
    # alertes et flux
    "alert_crash_content": {"en": "@here ⚠️ Server outage detected!", "fr": "@here ⚠️ Panne détectée sur un serveur !"},
    "alert_crash_title": {"en": "🚨 SERVER CRASH ALERT!", "fr": "🚨 ALERTE CRASH SERVEUR !"},
    "alert_crash_desc": {"en": "Server **{name}** just went **OFFLINE**!",
                         "fr": "Le serveur **{name}** vient de passer **HORS LIGNE** !"},
    "alert_back_title": {"en": "✅ Server is back", "fr": "✅ Serveur de retour"},
    "alert_back_desc": {"en": "Server **{name}** is **ONLINE** again.",
                        "fr": "Le serveur **{name}** est de nouveau **EN LIGNE**."},
    "alert_bl": {"en": "🚨 **BLACKLIST ALERT** 🚨: Player 🟥 **{p}**{tag} just connected on **{map}**!",
                 "fr": "🚨 **ALERTE BLACKLIST** 🚨 : Le joueur 🟥 **{p}**{tag} vient de se connecter sur **{map}** !"},
    "feed_join": {"en": "🟢 `[{t}]` **{p}**{tag} joined **{map}**", "fr": "🟢 `[{t}]` **{p}**{tag} a rejoint **{map}**"},
    "feed_leave": {"en": "🔴 `[{t}]` {pre}**{p}**{tag} left **{map}**{sess}",
                   "fr": "🔴 `[{t}]` {pre}**{p}**{tag} a quitté **{map}**{sess}"},
    "feed_session": {"en": " *(Session: {d})*", "fr": " *(Session : {d})*"},
    "raid_content": {"en": "@here 🛡️ PREPARE YOUR DEFENSES!", "fr": "@here 🛡️ PRÉPAREZ LES DÉFENSES !"},
    "raid_title": {"en": "🚨 RED ALERT: RAID / ATTACK INCOMING?", "fr": "🚨 ALERTE ROUGE : ATTAQUE / RAID IMMINENT ?"},
    "raid_desc": {"en": "**{n} blacklisted players** spotted simultaneously on **{map}**!\n\nPlayers: {names}",
                  "fr": "**{n} joueurs blacklistés** repérés en simultané sur **{map}** !\n\nJoueurs : {names}"},
    # dashboard
    "dash_offline": {"en": "🔴 **Offline**", "fr": "🔴 **Hors ligne**"},
    "dash_none": {"en": "No players", "fr": "Aucun joueur"},
    "dash_header": {"en": "🎮 **Players online**\n👤 **Total: {n} player(s)**\n------------------------------------",
                    "fr": "🎮 **Joueurs en ligne**\n👤 **Total : {n} joueur(s)**\n------------------------------------"},
    "dash_more": {"en": "… and {n} more server(s)", "fr": "… et {n} autre(s) serveur(s)"},
    "dash_footer": {"en": "{brand} • refreshed every {tick}s • 🟥 = Blacklist • [Tribe]",
                    "fr": "{brand} • actualisation toutes les {tick}s • 🟥 = Blacklist • [Tribu]"},
    # fiche des joueurs
    "roster_header": {"en": "📋 **Cluster players** — {n} player(s) · 🟢 {on} online\n*Sorted by playtime • top 2 maps*\n------------------------------------",
                      "fr": "📋 **Joueurs du cluster** — {n} joueur(s) · 🟢 {on} en ligne\n*Triés par temps de jeu • 2 maps les plus jouées*\n------------------------------------"},
    "roster_none": {"en": "No active players.", "fr": "Aucun joueur actif."},
    "roster_more": {"en": "\n… and {n} more", "fr": "\n… et {n} autre(s)"},
    "roster_footer": {"en": "Players inactive for {d}+ days are removed automatically • 🟥 = Blacklist • ▫️ = offline",
                      "fr": "Inactifs depuis +{d} jours retirés automatiquement • 🟥 = Blacklist • ▫️ = hors ligne"},
    # bilans
    "daily_title": {"en": "🌙 Daily recap & playtime", "fr": "🌙 Bilan quotidien & temps de jeu"},
    "daily_desc": {"en": "Activity report for **{date}**", "fr": "Rapport d'activité du **{date}**"},
    "daily_active": {"en": "👥 Unique active players today", "fr": "👥 Joueurs uniques actifs aujourd'hui"},
    "daily_bl": {"en": "🚨 Blacklisted players spotted today", "fr": "🚨 Joueurs de la blacklist repérés aujourd'hui"},
    "daily_bl_none": {"en": "No blacklisted player was seen.", "fr": "Aucun joueur blacklisté n'a été aperçu."},
    "daily_time": {"en": "⏱️ Playtime today", "fr": "⏱️ Temps de jeu de la journée"},
    "daily_footer": {"en": "{brand} • automatic recap at 23:00", "fr": "{brand} • bilan automatique de 23:00"},
    "weekly_title": {"en": "👑 NO-LIFE OF THE WEEK", "fr": "👑 LE NO-LIFE DE LA SEMAINE"},
    "weekly_desc": {"en": "Congratulations to **{p}**, who takes the tryhard crown!\n\n{podium}",
                    "fr": "Félicitations à **{p}** qui remporte la palme du tryhard !\n\n{podium}"},
    # bot
    "welcome_title": {"en": "👋 Thanks for adding {brand}!", "fr": "👋 Merci d'avoir ajouté {brand} !"},
    "welcome_desc": {"en": ("Set me up in **2 steps** *(Manage Server permission needed)*:\n\n"
                            "**1.** Pick the channel where players will show up\n"
                            "**2.** Add your game server (`IP:query port`)"),
                     "fr": ("Configure-moi en **2 étapes** *(permission Gérer le serveur requise)* :\n\n"
                            "**1.** Choisis le salon où afficher les joueurs\n"
                            "**2.** Ajoute ton serveur de jeu (`IP:port de requête`)")},
    "welcome_web": {"en": "👉 Easiest: click **Configure on the website** below.",
                    "fr": "👉 Le plus simple : clique sur **Configurer sur le site** ci-dessous."},
    "welcome_dc": {"en": "Or in Discord: `/config channels`, then `/config add-server`.",
                   "fr": "Ou dans Discord : `/config channels`, puis `/config add-server`."},
    "welcome_footer": {"en": "/setup shows this message again • En français : /config language",
                       "fr": "/setup réaffiche ce message"},
    "btn_web": {"en": "Configure on the website", "fr": "Configurer sur le site"},
    "btn_guide": {"en": "Tutorial", "fr": "Tuto"},
    "btn_support": {"en": "Support", "fr": "Support"},
    "setup_title": {"en": "🛠️ {brand} setup guide", "fr": "🛠️ Guide de configuration de {brand}"},
    "err_generic": {"en": "❌ An error occurred. Please try again in a moment.",
                    "fr": "❌ Une erreur est survenue. Réessaie dans un instant."},
    "err_admin": {"en": "⛔ Restricted to members with the “Manage Server” permission.",
                  "fr": "⛔ Réservé aux membres ayant la permission « Gérer le serveur »."},
    # /config
    "ch_perm": {"en": "❌ I'm missing **View Channel**, **Send Messages** and **Embed Links** in {ch}.",
                "fr": "❌ Je n'ai pas les droits **Voir le salon**, **Envoyer des messages** et **Intégrer des liens** dans {ch}."},
    "ch_dash": {"en": "✅ Dashboard: {ch}", "fr": "✅ Tableau de bord : {ch}"},
    "ch_hist": {"en": "\n✅ History: {ch}", "fr": "\n✅ Historique : {ch}"},
    "ch_nohist": {"en": "\nℹ️ No history channel (alerts and connection feed disabled).",
                  "fr": "\nℹ️ Pas de salon d'historique (alertes et connexions désactivées)."},
    "tz_unknown": {"en": "❌ Unknown time zone. Start typing (e.g. `Europe/Par`) and pick from the list.",
                   "fr": "❌ Fuseau inconnu. Commence à taper (ex : `Europe/Par`) et choisis dans la liste."},
    "tz_set": {"en": "✅ Time zone set to **{tz}**.", "fr": "✅ Fuseau horaire réglé sur **{tz}**."},
    "lang_set": {"en": "✅ Language set to **{name}**.", "fr": "✅ Langue réglée sur **{name}**."},
    "add_limit": {"en": "❌ Limit reached ({limit} servers on the {plan} plan).{up}",
                  "fr": "❌ Limite atteinte ({limit} serveurs pour l'offre {plan}).{up}"},
    "add_up": {"en": "\n⭐ Go **Premium** for up to {n} servers: {url}",
               "fr": "\n⭐ Passe en **Premium** pour aller jusqu'à {n} serveurs : {url}"},
    "add_badaddr": {"en": "❌ Invalid address. Use `IP:port`, for example `151.242.127.127:27018` (the port is your server's **query port**).",
                    "fr": "❌ Adresse invalide. Utilise `IP:port`, par exemple `151.242.127.127:27018` (le port est le **port de requête** de ton serveur)."},
    "add_exists": {"en": "❌ A server named **{name}** already exists.", "fr": "❌ Un serveur nommé **{name}** existe déjà."},
    "add_refused": {"en": "❌ Address refused: {err}. Use your server's public IP or domain.",
                    "fr": "❌ Adresse refusée : {err}. Utilise l'IP publique ou le domaine de ton serveur."},
    "add_ok": {"en": "✅ **{name}** added — server reachable ({c}/{m} players).",
               "fr": "✅ **{name}** ajouté — serveur joignable ({c}/{m} joueurs)."},
    "add_noresp": {"en": "⚠️ **{name}** added, but it isn't responding yet. Make sure you used the **query port** (often different from the game port) and that it isn't blocked by a firewall.",
                   "fr": "⚠️ **{name}** ajouté, mais il ne répond pas pour l'instant. Vérifie que tu as bien mis le **port de requête** (query port, souvent différent du port de jeu) et qu'il n'est pas bloqué par un pare-feu."},
    "rm_ok": {"en": "🗑️ **{name}** removed.", "fr": "🗑️ **{name}** retiré."},
    "rm_none": {"en": "❌ No server named **{name}**.", "fr": "❌ Aucun serveur nommé **{name}**."},
    "status_title": {"en": "⚙️ {brand} configuration", "fr": "⚙️ Configuration de {brand}"},
    "status_plan": {"en": "Plan", "fr": "Offre"},
    "status_plan_val": {"en": "**{plan}** • {n}/{max} servers", "fr": "**{plan}** • {n}/{max} serveurs"},
    "status_channels": {"en": "Channels", "fr": "Salons"},
    "status_channels_val": {"en": "Dashboard: {d}\nHistory: {h}", "fr": "Dashboard : {d}\nHistorique : {h}"},
    "status_tz": {"en": "Time zone", "fr": "Fuseau horaire"},
    "status_lang": {"en": "Language", "fr": "Langue"},
    "status_servers": {"en": "Game servers", "fr": "Serveurs de jeu"},
    "status_noservers": {"en": "*No server configured*", "fr": "*Aucun serveur configuré*"},
    "status_undefined": {"en": "❌ not set", "fr": "❌ non défini"},
    "status_noperm": {"en": "⚠️ missing permissions", "fr": "⚠️ droits manquants"},
    "status_premium": {"en": "⭐ Premium", "fr": "⭐ Premium"},
    "status_premium_val": {"en": "Player roster, raid alerts, daily/weekly recaps, more servers. {url}",
                           "fr": "Fiche des joueurs, alertes raid, bilans quotidiens/hebdo, plus de serveurs. {url}"},
    # blacklist
    "bl_full": {"en": "❌ Blacklist full ({n} players max).{up}", "fr": "❌ Blacklist pleine ({n} joueurs max).{up}"},
    "bl_up": {"en": "\n⭐ **Premium** allows up to {n} players: {url}",
              "fr": "\n⭐ L'offre **Premium** permet jusqu'à {n} joueurs : {url}"},
    "bl_added": {"en": "✅ 🟥 **{p}** added to the blacklist.", "fr": "✅ 🟥 **{p}** ajouté à la blacklist."},
    "bl_removed": {"en": "🗑️ **{p}** removed from the blacklist.", "fr": "🗑️ **{p}** retiré de la blacklist."},
    "bl_notin": {"en": "❌ **{p}** is not on the blacklist.", "fr": "❌ **{p}** n'est pas dans la blacklist."},
    "bl_empty": {"en": "📋 The blacklist is empty.", "fr": "📋 La blacklist est vide."},
    "bl_title": {"en": "🏴‍☠️ Blacklisted players", "fr": "🏴‍☠️ Joueurs blacklistés"},
    # tribes
    "tr_limit": {"en": "❌ Limit of {n} affiliations reached.", "fr": "❌ Limite de {n} affiliations atteinte."},
    "tr_set": {"en": "🏷️ **{p}** → tribe **[{t}]**.", "fr": "🏷️ **{p}** → tribu **[{t}]**."},
    "tr_removed": {"en": "🗑️ Affiliation of **{p}** removed.", "fr": "🗑️ Affiliation de **{p}** retirée."},
    "tr_none": {"en": "❌ No affiliation for **{p}**.", "fr": "❌ Aucune affiliation pour **{p}**."},
    "tr_empty": {"en": "📋 No affiliations recorded.", "fr": "📋 Aucune affiliation enregistrée."},
    "tr_title": {"en": "🛡️ Tribe directory", "fr": "🛡️ Répertoire des tribus"},
    # données
    "data_deleted": {"en": "🗑️ Data for **{p}** deleted ({n} record(s)). If the player is still connected, they'll be recorded again within a minute.",
                     "fr": "🗑️ Données de **{p}** supprimées ({n} enregistrement(s)). Si le joueur est encore connecté, il sera de nouveau enregistré à la prochaine minute."},
    "data_confirm": {"en": "❌ Type exactly `{word}` to confirm.", "fr": "❌ Tape exactement `{word}` pour confirmer."},
    "data_word": {"en": "DELETE", "fr": "SUPPRIMER"},
    "data_reset_ok": {"en": "🗑️ History and playtime erased.", "fr": "🗑️ Historique et temps de jeu effacés."},
    # commandes publiques
    "use_channel": {"en": "❌ Use this command in <#{id}>!", "fr": "❌ Utilise cette commande dans <#{id}> !"},
    "info_none": {"en": "❌ No playtime recorded for **{p}**.", "fr": "❌ Aucun temps de jeu enregistré pour **{p}**."},
    "info_title": {"en": "📊 Player profile: {bl}{p}{tag}", "fr": "📊 Fiche joueur : {bl}{p}{tag}"},
    "info_total": {"en": "⏱️ Total playtime", "fr": "⏱️ Temps de jeu total"},
    "info_maps": {"en": "🗺️ Breakdown by map", "fr": "🗺️ Répartition par map"},
    "hist_none": {"en": "❌ No results. Check the spelling (the start of a name is enough).",
                  "fr": "❌ Aucun résultat. Vérifie l'orthographe (un début de pseudo suffit)."},
    "hist_online": {"en": "**online now**", "fr": "**en ligne maintenant**"},
    "hist_when": {"en": "**{ago}** · {date} at {time}", "fr": "**{ago}** · {date} à {time}"},
    "hist_more_maps": {"en": "└ *+{n} more map(s)*", "fr": "└ *+{n} autre(s) map(s)*"},
    "hist_n_players": {"en": "{n} player(s)", "fr": "{n} joueur(s)"},
    "hist_n_maps": {"en": "{n} map(s)", "fr": "{n} map(s)"},
    "hist_map_head": {"en": "🗺️ **{m}** — {n} player(s)", "fr": "🗺️ **{m}** — {n} joueur(s)"},
    "hist_more": {"en": "└ *+{n} more*", "fr": "└ *+{n} autre(s)*"},
    "hist_crit_player": {"en": "player “{p}”", "fr": "joueur « {p} »"},
    "hist_crit_map": {"en": "map “{m}”", "fr": "map « {m} »"},
    "hist_crit_default": {"en": "latest connections", "fr": "dernières connexions"},
    "hist_title": {"en": "🔍 Connection history", "fr": "🔍 Historique des connexions"},
    "hist_footer": {"en": "Times in {tz} • 🟢 online • ▫️ offline • 🟥 blacklist",
                    "fr": "Heures en {tz} • 🟢 en ligne • ▫️ hors ligne • 🟥 blacklist"},
    "stats_title": {"en": "📊 Statistics", "fr": "📊 Statistiques"},
    "stats_unique": {"en": "👥 Unique players recorded", "fr": "👥 Joueurs uniques enregistrés"},
    "stats_servers": {"en": "🖥️ Game servers tracked", "fr": "🖥️ Serveurs de jeu suivis"},
    "stats_bl": {"en": "🟥 Blacklisted players", "fr": "🟥 Joueurs blacklistés"},
    "stats_tribes": {"en": "🛡️ Tribe affiliations", "fr": "🛡️ Affiliations de tribu"},
    "help_title": {"en": "❓ Help — {brand}", "fr": "❓ Aide — {brand}"},
    "help_desc": {"en": ("**For everyone**\n"
                         "`/history` — last connection of a player on each map\n"
                         "`/info` — a player's playtime\n"
                         "`/stats` — Discord server statistics\n"
                         "`/setup` — step-by-step setup guide\n\n"
                         "**For admins** (*Manage Server* permission)\n"
                         "`/config channels` • `/config add-server` • `/config remove-server` • `/config timezone` • `/config language` • `/config status`\n"
                         "`/blacklist add|remove|list` • `/tribe set|remove|list`\n"
                         "`/data delete-player` • `/data reset`"),
                  "fr": ("**Pour tout le monde**\n"
                         "`/history` — dernière connexion d'un joueur sur chaque map\n"
                         "`/info` — temps de jeu d'un joueur\n"
                         "`/stats` — statistiques du serveur Discord\n"
                         "`/setup` — guide de configuration pas à pas\n\n"
                         "**Pour les administrateurs** (permission *Gérer le serveur*)\n"
                         "`/config channels` • `/config add-server` • `/config remove-server` • `/config timezone` • `/config language` • `/config status`\n"
                         "`/blacklist add|remove|list` • `/tribe set|remove|list`\n"
                         "`/data delete-player` • `/data reset`")},
    "help_links": {"en": "Links", "fr": "Liens"},
    "help_site": {"en": "Website", "fr": "Site web"},
    "sep": {"en": "\n", "fr": "\n"},
    "help_privacy": {"en": "Privacy", "fr": "Confidentialité"},
    "help_support": {"en": "Support", "fr": "Support"},
}


def tr(lang, key, **kw):
    entry = STR[key]
    text = entry.get(lang) or entry["en"]
    return text.format(**kw) if kw else text


def plan_label(lang, plan):
    return tr(lang, "plan_premium" if plan == "premium" else "plan_free")


# Valeurs enregistrées en base (indépendantes de la langue). Anciennes valeurs françaises acceptées.
ST_ON, ST_OFF, ST_UNK = "online", "offline", "unknown"
_LEGACY_ST = {"En ligne": ST_ON, "Hors ligne": ST_OFF, "Inconnu": ST_UNK}


def norm_status(value):
    return _LEGACY_ST.get(value, value) or ST_UNK



# ======================================================================================
# BASE DE DONNÉES (SQLite en local, PostgreSQL si DATABASE_URL est défini)
# ======================================================================================
_pool = None


def _connect():
    global _pool
    if IS_PG:
        if _pool is None:
            _pool = pg_pool.ThreadedConnectionPool(1, 8, DATABASE_URL)
        conn = _pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute(f"SET search_path TO {DB_SCHEMA}")     # nos tables vivent dans leur propre schéma
            conn.commit()
            cur.close()
        except Exception:
            _pool.putconn(conn, close=True)
            raise
        return conn
    return sqlite3.connect(DB_NAME, timeout=30)


def _release(conn, broken=False):
    if IS_PG:
        _pool.putconn(conn, close=broken)
    else:
        conn.close()


def _prep(sql):
    return sql.replace("?", "%s") if IS_PG else sql


def _run(fn):
    conn = _connect()
    broken = False
    try:
        cur = conn.cursor()
        try:
            res = fn(cur)
            conn.commit()
            return res
        finally:
            cur.close()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            broken = True
        raise
    finally:
        _release(conn, broken)


def db_execute(sql, params=(), fetch=None):
    def fn(cur):
        cur.execute(_prep(sql), params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()
        if fetch == "rowcount":
            return cur.rowcount
        return None
    return _run(fn)


def db_executemany(sql, rows):
    rows = list(rows)
    if not rows:
        return

    def fn(cur):
        if IS_PG:
            execute_batch(cur, _prep(sql), rows, page_size=200)
        else:
            cur.executemany(sql, rows)
    _run(fn)


async def adb(sql, params=(), fetch=None):
    return await asyncio.to_thread(db_execute, sql, params, fetch)


async def adb_many(sql, rows):
    return await asyncio.to_thread(db_executemany, sql, rows)


def init_db():
    queries = [
        "CREATE TABLE IF NOT EXISTS guilds (guild_id BIGINT PRIMARY KEY, channel_id BIGINT, history_channel_id BIGINT, "
        "tz TEXT, plan TEXT NOT NULL DEFAULT 'free', dashboard_id BIGINT, roster_ids TEXT, last_daily TEXT, "
        "last_weekly TEXT, created_at TEXT, left_at TEXT, lang TEXT)",
        "CREATE TABLE IF NOT EXISTS servers (guild_id BIGINT, name TEXT, host TEXT, port INTEGER, status TEXT, "
        "PRIMARY KEY (guild_id, name))",
        "CREATE TABLE IF NOT EXISTS blacklist (guild_id BIGINT, player_name TEXT, PRIMARY KEY (guild_id, player_name))",
        "CREATE TABLE IF NOT EXISTS player_tribes (guild_id BIGINT, player_name TEXT, tribe_name TEXT, "
        "PRIMARY KEY (guild_id, player_name))",
        "CREATE TABLE IF NOT EXISTS playtime (guild_id BIGINT, player_name TEXT, map_name TEXT, date TEXT, "
        "seconds INTEGER, PRIMARY KEY (guild_id, player_name, map_name, date))",
        "CREATE TABLE IF NOT EXISTS history (guild_id BIGINT, player_name TEXT, map_name TEXT, first_seen TEXT, "
        "last_seen TEXT, PRIMARY KEY (guild_id, player_name, map_name))",
        "CREATE INDEX IF NOT EXISTS idx_playtime_gdate ON playtime(guild_id, date)",
        "CREATE TABLE IF NOT EXISTS web_sessions (token_hash TEXT PRIMARY KEY, user_id BIGINT, username TEXT, "
        "avatar TEXT, guilds TEXT, expires_at TEXT)",
    ]
    if IS_PG:
        db_execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
        cur_schema = db_execute("SELECT current_schema()", fetch="one")[0]
        if cur_schema != DB_SCHEMA:             # vérifié AVANT de créer les tables, pour ne rien mélanger
            raise SystemExit(f"Schéma actif « {cur_schema} » au lieu de « {DB_SCHEMA} » : utilise le Session pooler (port 5432).")
    for q in queries:
        db_execute(q)
    try:                                         # migration : colonne « lang » ajoutée après la première version
        db_execute("ALTER TABLE guilds ADD COLUMN lang TEXT")
    except Exception:
        pass
    if IS_PG:
        # Sécurité (Supabase expose les tables par une API publique) : RLS activé sans règle = accès API refusé.
        # Le bot se connecte avec le propriétaire des tables, qui n'est pas concerné.
        for table in ("guilds", "servers", "blacklist", "player_tribes", "playtime", "history", "web_sessions"):
            try:
                db_execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            except Exception as exc:
                log.warning("RLS non activé sur %s : %s", table, exc)
    else:
        db_execute("PRAGMA journal_mode=WAL")


init_db()

UPSERT_HISTORY = (
    "INSERT INTO history (guild_id, player_name, map_name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT(guild_id, player_name, map_name) DO UPDATE SET last_seen = excluded.last_seen"
)
UPSERT_PLAYTIME = (
    "INSERT INTO playtime (guild_id, player_name, map_name, date, seconds) VALUES (?, ?, ?, ?, ?) "
    "ON CONFLICT(guild_id, player_name, map_name, date) DO UPDATE SET seconds = playtime.seconds + excluded.seconds"
)
UPDATE_STATUS = "UPDATE servers SET status = ? WHERE guild_id = ? AND name = ?"

GUILD_COLS = ("guild_id, channel_id, history_channel_id, tz, plan, dashboard_id, roster_ids, "
              "last_daily, last_weekly, lang")


def row_to_guild(r):
    g = dict(zip(GUILD_COLS.split(", "), r))
    g["lang"] = g.get("lang") if g.get("lang") in LANGS else DEFAULT_LANG
    return g


async def ensure_guild(gid):
    await adb("INSERT INTO guilds (guild_id, tz, plan, created_at) VALUES (?, ?, 'free', ?) "
              "ON CONFLICT(guild_id) DO NOTHING", (gid, DEFAULT_TZ, utcnow().isoformat()))
    await adb("UPDATE guilds SET left_at = NULL WHERE guild_id = ? AND left_at IS NOT NULL", (gid,))


async def get_guild(gid):
    row = await adb(f"SELECT {GUILD_COLS} FROM guilds WHERE guild_id = ?", (gid,), fetch="one")
    if not row:
        await ensure_guild(gid)
        row = await adb(f"SELECT {GUILD_COLS} FROM guilds WHERE guild_id = ?", (gid,), fetch="one")
    return row_to_guild(row)


def delete_guild_data(gid):
    for table in ("servers", "blacklist", "player_tribes", "playtime", "history", "guilds"):
        db_execute(f"DELETE FROM {table} WHERE guild_id = ?", (gid,))


# ======================================================================================
# OUTILS
# ======================================================================================
def like(text):
    return "%" + text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def like_clause(col):
    return f"LOWER({col}) LIKE LOWER(?) ESCAPE '\\'"


def nm(text):
    """Neutralise le markdown et les mentions dans un texte venant d'un joueur ou d'un utilisateur."""
    return discord.utils.escape_markdown(discord.utils.escape_mentions(str(text)))


def fmt_duration(total_sec):
    total_sec = int(total_sec or 0)
    h, m = total_sec // 3600, (total_sec % 3600) // 60
    return f"{h}h {m:02d}m" if h > 0 else f"{m}m"


def fmt_ago(secs, lang="en"):
    secs = max(int(secs), 0)
    if secs < 120:
        return tr(lang, "ago_now")
    if secs < 3600:
        return tr(lang, "ago_min", n=secs // 60)
    if secs < 86400:
        return tr(lang, "ago_h", h=secs // 3600, m=(secs % 3600) // 60)
    return tr(lang, "ago_d", n=secs // 86400)


def parse_ts(ls, tz):
    try:
        dt = datetime.fromisoformat(str(ls))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)


def tz_of(g):
    try:
        return ZoneInfo(g.get("tz") or DEFAULT_TZ)
    except Exception:
        return ZoneInfo("UTC")


def is_blacklisted(player, blacklist):
    return player.lower() in blacklist


def tribe_tag(player, tribes):
    t = tribes.get(player.lower())
    return f" [{nm(t)}]" if t else ""


def pack_blocks(blocks, lang="en", limit=3900):
    out, size = [], 0
    for i, blk in enumerate(blocks):
        if size + len(blk) + 2 > limit:
            out.append(tr(lang, "pack_more", n=len(blocks) - i))
            break
        out.append(blk)
        size += len(blk) + 2
    return "\n\n".join(out)


def can_write(channel):
    if channel is None:
        return False
    try:
        p = channel.permissions_for(channel.guild.me)
    except Exception:
        return False
    return p.view_channel and p.send_messages and p.embed_links


_warned = set()


async def safe_send(channel, *args, allowed_mentions=NO_MENTIONS, **kwargs):
    try:
        return await channel.send(*args, allowed_mentions=allowed_mentions, **kwargs)
    except discord.HTTPException as exc:
        key = (getattr(channel, "id", 0), exc.status)
        if key not in _warned:                      # évite de spammer les logs
            _warned.add(key)
            log.warning("Envoi impossible dans %s : %s", getattr(channel, "id", "?"), exc)
        return None


async def send_chunks(channel, lines, limit=1900):
    buf = ""
    for line in lines:
        if buf and len(buf) + len(line) + 1 > limit:
            await safe_send(channel, buf)
            buf = ""
        buf += ("\n" if buf else "") + line
    if buf:
        await safe_send(channel, buf)


# ======================================================================================
# REQUÊTES VERS LES SERVEURS DE JEU (avec protection anti-abus)
# ======================================================================================
HOST_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9\-\.]{0,251}[A-Za-z0-9])?$")


def parse_address(text):
    """'1.2.3.4:27015' ou 'mon.domaine.fr:27015' -> (hôte, port). Lève ValueError si invalide."""
    t = text.strip().lower()
    t = re.sub(r"^[a-z]+://", "", t).split("/")[0]
    if t.count(":") != 1:
        raise ValueError("format")
    host, port_s = t.split(":")
    if not host or not port_s.isdigit() or not (1024 <= int(port_s) <= 65535):
        raise ValueError("format")
    return host, int(port_s)


def resolve_public(host):
    """Résout l'hôte en IPv4 et refuse toute adresse non publique (réseau local, loopback, etc.)."""
    if not HOST_RE.match(host):
        raise ValueError("invalid address")
    infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_DGRAM)
    ips = sorted({i[4][0] for i in infos})
    if not ips:
        raise ValueError("host not found")
    for ip in ips:
        if not ipaddress.ip_address(ip).is_global:
            raise ValueError("non-public address refused")
    return ips[0]


def query_server(host, port):
    last_err = None
    for _ in range(2):
        try:
            addr = (resolve_public(host), port)
            info = a2s.info(addr, timeout=3)
            players = a2s.players(addr, timeout=3)
            names = [p.name.strip() for p in players if getattr(p, "name", "") and p.name.strip()]
            return {"online": True, "players": names, "max_players": getattr(info, "max_players", 0), "error": None}
        except Exception as exc:
            last_err = str(exc)
    print(f"[A2S] échec {host}:{port} -> {last_err}", flush=True)
    return {"online": False, "players": [], "max_players": 0, "error": last_err}


async def query_all(targets):
    """Chaque (hôte, port) distinct n'est interrogé qu'une fois, même s'il est suivi par plusieurs Discord."""
    sem = asyncio.Semaphore(20)

    async def one(t):
        async with sem:
            return t, await asyncio.to_thread(query_server, t[0], t[1])

    return dict(await asyncio.gather(*(one(t) for t in targets)))


# ======================================================================================
# ÉTAT EN MÉMOIRE (clé = (guild_id, nom du serveur de jeu))
# ======================================================================================
previous_players = {}      # (gid, serveur) -> set(joueurs)
session_starts = {}        # (gid, serveur, joueur) -> datetime
fail_streak = {}           # (gid, serveur) -> int
last_max = {}              # (gid, serveur) -> int
recent_bl = {}             # gid -> [{"player","map","time"}]
gstate = {}                # gid -> état d'affichage (dashboard / fiche)
tick_no = 0


def st(g):
    s = gstate.setdefault(g["guild_id"], {"loaded": False, "dashboard_id": None, "roster_ids": [],
                                          "roster_sigs": [], "roster_last": None})
    if not s["loaded"]:
        s["loaded"] = True
        s["dashboard_id"] = g.get("dashboard_id")
        s["roster_ids"] = [int(x) for x in (g.get("roster_ids") or "").split(",") if x.strip().isdigit()]
    return s


def forget_server(gid, name):
    previous_players.pop((gid, name), None)
    fail_streak.pop((gid, name), None)
    last_max.pop((gid, name), None)
    for k in [k for k in session_starts if k[0] == gid and k[1] == name]:
        session_starts.pop(k, None)


def forget_guild(gid):
    for d in (previous_players, fail_streak, last_max):
        for k in [k for k in d if k[0] == gid]:
            d.pop(k, None)
    for k in [k for k in session_starts if k[0] == gid]:
        session_starts.pop(k, None)
    recent_bl.pop(gid, None)
    gstate.pop(gid, None)


def online_now(gid):
    return {(m, p) for (g, m), ps in previous_players.items() if g == gid for p in ps}


# ======================================================================================
# CŒUR : calcul d'un relevé pour UN serveur Discord (aucune entrée/sortie réseau ici)
# ======================================================================================
def compute_guild(g, servers, results, blacklist, tribes, now_utc, out):
    gid, lang = g["guild_id"], g["lang"]
    now = now_utc.astimezone(tz_of(g))
    today, time_str = now.strftime("%Y-%m-%d"), now.strftime("%H:%M")
    now_iso = now_utc.isoformat()
    views, feed, alerts, changed = [], [], [], False

    for s in servers:
        name, key = s["name"], (gid, s["name"])
        res = results[(s["host"], s["port"])]
        prev_status = norm_status(s["status"])

        if res["online"]:
            fail_streak[key] = 0
            new_status = ST_ON
            last_max[key] = res["max_players"]
        else:
            fail_streak[key] = fail_streak.get(key, 0) + 1
            new_status = ST_OFF if fail_streak[key] >= FAIL_THRESHOLD else prev_status

        if prev_status == ST_ON and new_status == ST_OFF:
            alerts.append((tr(lang, "alert_crash_content"), discord.Embed(
                title=tr(lang, "alert_crash_title"),
                description=tr(lang, "alert_crash_desc", name=nm(name)),
                color=discord.Color.red()), True))
        elif prev_status == ST_OFF and new_status == ST_ON:
            alerts.append((None, discord.Embed(
                title=tr(lang, "alert_back_title"),
                description=tr(lang, "alert_back_desc", name=nm(name)),
                color=discord.Color.green()), False))
        if new_status != s["status"]:
            out["status"].append((new_status, gid, name))
            s["status"] = new_status

        if res["online"]:
            views.append({"name": name, "online": True, "players": res["players"], "max": res["max_players"]})
        elif new_status == ST_ON:                 # échec isolé : on garde l'affichage précédent
            views.append({"name": name, "online": True, "players": sorted(previous_players.get(key, set())),
                          "max": last_max.get(key, 0)})
        else:
            views.append({"name": name, "online": False, "players": [], "max": 0})

        if not res["online"] and fail_streak[key] < FAIL_THRESHOLD:
            continue

        current = set(res["players"]) if res["online"] else set()
        if key in previous_players:
            joined, left = current - previous_players[key], previous_players[key] - current
        else:                                      # premier passage : on initialise sans annoncer
            joined, left = set(), set()
            for p in current:
                session_starts[(gid, name, p)] = now
        changed = changed or bool(joined or left)

        for p in current:
            out["history"].append((gid, p, name, now_iso, now_iso))
            out["playtime"].append((gid, p, name, today, TICK))

        for p in sorted(joined):
            session_starts[(gid, name, p)] = now
            tag = tribe_tag(p, tribes)
            if is_blacklisted(p, blacklist):
                recent_bl.setdefault(gid, []).append({"player": p, "map": name, "time": now})
                alerts.append((tr(lang, "alert_bl", p=nm(p), tag=tag, map=nm(name)), None, False))
            else:
                feed.append(tr(lang, "feed_join", t=time_str, p=nm(p), tag=tag, map=nm(name)))

        for p in sorted(left):
            start = session_starts.pop((gid, name, p), None)
            sess = tr(lang, "feed_session", d=fmt_duration((now - start).total_seconds())) if start else ""
            pre = "🟥 " if is_blacklisted(p, blacklist) else ""
            feed.append(tr(lang, "feed_leave", t=time_str, pre=pre, p=nm(p), tag=tribe_tag(p, tribes),
                           map=nm(name), sess=sess))

        previous_players[key] = current

    # alerte raid : 3+ blacklistés sur la même map en 5 min (offre premium)
    if feat(g, "raid"):
        limit = now - timedelta(minutes=5)
        lst = [c for c in recent_bl.get(gid, []) if c["time"] >= limit]
        by_map = {}
        for c in lst:
            by_map.setdefault(c["map"], set()).add(c["player"])
        for map_name, players in by_map.items():
            if len(players) >= 3:
                names = ", ".join(f"🟥 **{nm(p)}**" for p in sorted(players))
                alerts.append((tr(lang, "raid_content"), discord.Embed(
                    title=tr(lang, "raid_title"),
                    description=tr(lang, "raid_desc", n=len(players), map=nm(map_name), names=names),
                    color=discord.Color.dark_magenta()), True))
                lst = [c for c in lst if c["map"] != map_name]
        recent_bl[gid] = lst
    else:
        recent_bl[gid] = []

    return {"now": now, "views": views, "feed": feed, "alerts": alerts, "changed": changed}


# ======================================================================================
# AFFICHAGE : DASHBOARD
# ======================================================================================
def build_dashboard_embeds(views, blacklist, tribes, lang):
    total = sum(len(v["players"]) for v in views if v["online"])
    blocks = []
    for v in views:
        if not v["online"]:
            blocks.append(f"🗺️ **{nm(v['name'])}**\n{tr(lang, 'dash_offline')}")
            continue
        if v["players"]:
            parts = []
            for p in sorted(v["players"], key=str.lower):
                parts.append(f"🟥 **{nm(p)}**{tribe_tag(p, tribes)}" if is_blacklisted(p, blacklist)
                             else f"{nm(p)}{tribe_tag(p, tribes)}")
            plist = ", ".join(parts)
            if len(plist) > 700:
                plist = plist[:690] + "…"
        else:
            plist = tr(lang, "dash_none")
        blocks.append(f"🗺️ **{nm(v['name'])}**\n🟢 **{len(v['players'])}/{v['max']}** — {plist}")

    header = tr(lang, "dash_header", n=total)
    pages, buf = [], header
    for i, blk in enumerate(blocks):
        if len(buf) + len(blk) + 1 > 2800:
            pages.append(buf)
            buf = ""
            if len(pages) == 2:                    # 6000 caractères max par message Discord
                buf = tr(lang, "dash_more", n=len(blocks) - i)
                break
        buf += ("\n" if buf else "") + blk
    pages.append(buf)
    embeds = [discord.Embed(description=t, color=discord.Color.blue()) for t in pages]
    embeds[-1].set_footer(text=tr(lang, "dash_footer", brand=BRAND, tick=TICK))
    return embeds


async def update_dashboard(g, channel, embeds):
    s = st(g)
    gid = g["guild_id"]
    if s["dashboard_id"]:
        try:
            await channel.get_partial_message(s["dashboard_id"]).edit(embeds=embeds)
            return
        except discord.NotFound:
            s["dashboard_id"] = None
        except discord.HTTPException as exc:
            log.warning("Dashboard edit failed (%s): %s", gid, exc)
            return
    msg = await safe_send(channel, embeds=embeds)
    if msg:
        s["dashboard_id"] = msg.id
        await adb("UPDATE guilds SET dashboard_id = ? WHERE guild_id = ?", (msg.id, gid))


# ======================================================================================
# AFFICHAGE : FICHE DES JOUEURS DU CLUSTER (offre premium)
# ======================================================================================
MAX_ROSTER_PAGES = 5
PAGE_CHARS = 3900


async def build_roster_embeds(gid, tz, blacklist, tribes, online, lang):
    cutoff = (utcnow() - timedelta(days=INACTIVE_DAYS)).isoformat()
    history, playtime = await asyncio.gather(
        adb("SELECT player_name, map_name, last_seen FROM history WHERE guild_id = ?", (gid,), fetch="all"),
        adb("SELECT player_name, map_name, SUM(seconds) FROM playtime WHERE guild_id = ? "
            "GROUP BY player_name, map_name", (gid,), fetch="all"),
    )
    latest = {}
    for p, m, ls in history or []:
        ls = str(ls)
        if p not in latest or ls > latest[p][0]:
            latest[p] = (ls, m)
    per_player = {}
    for p, m, sec in playtime or []:
        per_player.setdefault(p, []).append((sec or 0, m))

    def total(p):
        return sum(x for x, _ in per_player.get(p, []))

    active = [p for p, (ls, _) in latest.items() if ls >= cutoff]
    active.sort(key=lambda p: (-total(p), p.lower()))
    lines = []
    for p in active:
        top = sorted(per_player.get(p, []), reverse=True)[:2]
        maps_txt = " · ".join(f"{nm(m)} `{fmt_duration(x)}`" for x, m in top) if top else nm(latest[p][1])
        dot = "🟥" if is_blacklisted(p, blacklist) else ("🟢" if p in online else "▫️")
        lines.append(f"{dot} **{nm(p)}**{tribe_tag(p, tribes)} — {maps_txt}")

    n_online = sum(1 for p in active if p in online)
    header = tr(lang, "roster_header", n=len(active), on=n_online)
    if not lines:
        lines = [tr(lang, "roster_none")]
    pages, buf = [], header
    for line in lines:
        if len(buf) + len(line) + 1 > PAGE_CHARS:
            pages.append(buf)
            buf = ""
        buf += ("\n" if buf else "") + line
    pages.append(buf)
    if len(pages) > MAX_ROSTER_PAGES:
        hidden = sum(pg.count("\n") + 1 for pg in pages[MAX_ROSTER_PAGES:])
        pages = pages[:MAX_ROSTER_PAGES]
        pages[-1] += tr(lang, "roster_more", n=hidden)
    embeds = [discord.Embed(description=t, color=discord.Color.purple()) for t in pages]
    embeds[-1].set_footer(text=tr(lang, "roster_footer", d=INACTIVE_DAYS))
    return embeds


async def update_roster(g, channel, embeds):
    """Fiche affichée AVANT le dashboard : si le nombre de pages change, tout est recréé dans l'ordre."""
    s, gid = st(g), g["guild_id"]
    sigs = [e.description for e in embeds]
    now = utcnow()
    ids = s["roster_ids"]

    if ids and len(ids) == len(embeds):
        if sigs == s["roster_sigs"] and s["roster_last"] and (now - s["roster_last"]).total_seconds() < 600:
            return
        try:
            for mid, emb in zip(ids, embeds):
                await channel.get_partial_message(mid).edit(embed=emb)
            s["roster_sigs"], s["roster_last"] = sigs, now
            return
        except discord.NotFound:
            pass
        except discord.HTTPException as exc:
            log.warning("Édition de la fiche impossible (%s) : %s", gid, exc)
            return

    for mid in ids:
        try:
            await channel.get_partial_message(mid).delete()
        except discord.HTTPException:
            pass
    if s["dashboard_id"]:
        try:
            await channel.get_partial_message(s["dashboard_id"]).delete()
        except discord.HTTPException:
            pass
        s["dashboard_id"] = None
    new_ids = []
    for emb in embeds:
        msg = await safe_send(channel, embed=emb)
        if not msg:
            break
        new_ids.append(msg.id)
    s["roster_ids"], s["roster_sigs"], s["roster_last"] = new_ids, sigs, now
    await adb("UPDATE guilds SET roster_ids = ?, dashboard_id = NULL WHERE guild_id = ?",
              (",".join(str(i) for i in new_ids), gid))


# ======================================================================================
# BILANS (heure locale de chaque serveur Discord — offre premium)
# ======================================================================================
async def send_daily(g, hchan, blacklist, tribes, now):
    gid, today, lang = g["guild_id"], now.strftime("%Y-%m-%d"), g["lang"]
    rows = (await adb("SELECT player_name, map_name, seconds FROM playtime WHERE guild_id = ? AND date = ?",
                      (gid, today), fetch="all")) or []
    active = {}
    for player, map_name, sec in rows:
        info = active.setdefault(player, {"servers": set(), "seconds": 0})
        info["servers"].add(map_name)
        info["seconds"] += sec or 0
    detected = [p for p in active if is_blacklisted(p, blacklist)]
    embed = discord.Embed(title=tr(lang, "daily_title"),
                          description=tr(lang, "daily_desc", date=today), color=discord.Color.gold())
    embed.add_field(name=tr(lang, "daily_active"), value=str(len(active)), inline=False)
    embed.add_field(name=tr(lang, "daily_bl"),
                    value="\n".join(f"🟥 **{nm(p)}**{tribe_tag(p, tribes)}" for p in detected)[:1024]
                    or tr(lang, "daily_bl_none"), inline=False)
    if active:
        top = sorted(active.items(), key=lambda x: x[1]["seconds"], reverse=True)[:15]
        lines = []
        for p, info in top:
            pre = "🟥 " if is_blacklisted(p, blacklist) else "• "
            lines.append(f"{pre}**{nm(p)}**{tribe_tag(p, tribes)} — ⏱️ `{fmt_duration(info['seconds'])}` "
                         f"(*{nm(', '.join(sorted(info['servers'])))}*)")
        embed.add_field(name=tr(lang, "daily_time"), value="\n".join(lines)[:1024], inline=False)
    embed.set_footer(text=tr(lang, "daily_footer", brand=BRAND))
    await safe_send(hchan, embed=embed)


async def send_weekly(g, hchan, now):
    lang = g["lang"]
    since = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    rows = (await adb("SELECT player_name, SUM(seconds) AS total FROM playtime WHERE guild_id = ? AND date >= ? "
                      "GROUP BY player_name ORDER BY total DESC LIMIT 3", (g["guild_id"], since), fetch="all")) or []
    if not rows:
        return
    medals = ["🥇", "🥈", "🥉"]
    podium = "\n".join(f"{medals[i]} **{nm(n)}** — `{fmt_duration(t)}`" for i, (n, t) in enumerate(rows))
    await safe_send(hchan, embed=discord.Embed(
        title=tr(lang, "weekly_title"),
        description=tr(lang, "weekly_desc", p=nm(rows[0][0]), podium=podium),
        color=discord.Color.gold()))


async def maybe_summaries(g, hchan, blacklist, tribes, now):
    if not hchan or not feat(g, "summaries"):
        return
    today, gid = now.strftime("%Y-%m-%d"), g["guild_id"]
    if now.hour == 23 and g.get("last_daily") != today:
        g["last_daily"] = today
        await adb("UPDATE guilds SET last_daily = ? WHERE guild_id = ?", (today, gid))
        await send_daily(g, hchan, blacklist, tribes, now)
    if now.weekday() == 6 and now.hour == 20 and g.get("last_weekly") != today:
        g["last_weekly"] = today
        await adb("UPDATE guilds SET last_weekly = ? WHERE guild_id = ?", (today, gid))
        await send_weekly(g, hchan, now)


# ======================================================================================
# BOUCLE DE SURVEILLANCE
# ======================================================================================
async def deliver(g, plan, blacklist, tribes):
    guild = bot.get_guild(g["guild_id"])
    if not guild:
        return
    channel = guild.get_channel(g["channel_id"]) if g.get("channel_id") else None
    hchan = guild.get_channel(g["history_channel_id"]) if g.get("history_channel_id") else None
    channel = channel if can_write(channel) else None
    hchan = hchan if can_write(hchan) else None
    s = st(g)

    if hchan:
        for content, embed, ping in plan["alerts"]:
            mentions = PING_HERE if ping else NO_MENTIONS
            if embed:
                await safe_send(hchan, content, embed=embed, allowed_mentions=mentions)
            elif content:
                await safe_send(hchan, content)
        await send_chunks(hchan, plan["feed"])

    if channel:
        if feat(g, "roster") and (plan["changed"] or s["roster_last"] is None or tick_no % 5 == 0):
            try:
                online = {p for v in plan["views"] if v["online"] for p in v["players"]}
                embeds = await build_roster_embeds(g["guild_id"], tz_of(g), blacklist, tribes, online, g["lang"])
                await update_roster(g, channel, embeds)
            except Exception:
                log.exception("Erreur fiche joueurs (%s)", g["guild_id"])
        await update_dashboard(g, channel, build_dashboard_embeds(plan["views"], blacklist, tribes, g["lang"]))

    await maybe_summaries(g, hchan, blacklist, tribes, plan["now"])


async def run_tick():
    global tick_no
    tick_no += 1
    now_utc = utcnow()
    rows = (await adb(f"SELECT {GUILD_COLS} FROM guilds WHERE left_at IS NULL", fetch="all")) or []
    guilds = {g["guild_id"]: g for g in map(row_to_guild, rows) if bot.get_guild(g["guild_id"])}
    if not guilds:
        return

    srv_rows, bl_rows, tr_rows = await asyncio.gather(
        adb("SELECT guild_id, name, host, port, status FROM servers ORDER BY guild_id, name", fetch="all"),
        adb("SELECT guild_id, player_name FROM blacklist", fetch="all"),
        adb("SELECT guild_id, player_name, tribe_name FROM player_tribes", fetch="all"),
    )
    servers, blacklists, tribes_by = {}, {}, {}
    for gid, name, host, port, status in srv_rows or []:
        if gid in guilds:
            servers.setdefault(gid, []).append({"name": name, "host": host, "port": port, "status": status})
    for gid, p in bl_rows or []:
        blacklists.setdefault(gid, set()).add(p.lower())
    for gid, p, t in tr_rows or []:
        tribes_by.setdefault(gid, {})[p.lower()] = t
    if not servers:
        return

    targets = {(s["host"], s["port"]) for lst in servers.values() for s in lst}
    results = await query_all(targets)

    out = {"status": [], "history": [], "playtime": []}
    plans = {}
    for gid, lst in servers.items():
        try:
            plans[gid] = compute_guild(guilds[gid], lst, results, blacklists.get(gid, set()),
                                       tribes_by.get(gid, {}), now_utc, out)
        except Exception:
            log.exception("Erreur de calcul (%s)", gid)

    await adb_many(UPDATE_STATUS, out["status"])
    await adb_many(UPSERT_HISTORY, out["history"])
    await adb_many(UPSERT_PLAYTIME, out["playtime"])

    sem = asyncio.Semaphore(5)

    async def one(gid):
        async with sem:
            try:
                await deliver(guilds[gid], plans[gid], blacklists.get(gid, set()), tribes_by.get(gid, {}))
            except Exception:
                log.exception("Erreur d'envoi (%s)", gid)

    await asyncio.gather(*(one(gid) for gid in plans))


@tasks.loop(seconds=TICK)
async def monitor():
    try:
        await run_tick()
    except Exception:
        log.exception("Erreur dans la boucle de surveillance (ignorée)")


@tasks.loop(hours=24)
async def maintenance():
    """Purge automatique (confidentialité) : anciennes données + serveurs Discord qui ont retiré le bot."""
    try:
        cut = utcnow() - timedelta(days=RETENTION_DAYS)
        await adb("DELETE FROM playtime WHERE date < ?", (cut.strftime("%Y-%m-%d"),))
        await adb("DELETE FROM history WHERE last_seen < ?", (cut.isoformat(),))
        await adb("DELETE FROM web_sessions WHERE expires_at < ?", (utcnow().isoformat(),))
        gone = (utcnow() - timedelta(days=LEFT_GRACE_DAYS)).isoformat()
        rows = (await adb("SELECT guild_id FROM guilds WHERE left_at IS NOT NULL AND left_at < ?", (gone,),
                          fetch="all")) or []
        for (gid,) in rows:
            await asyncio.to_thread(delete_guild_data, gid)
            forget_guild(gid)
        log.info("Maintenance : %d serveur(s) Discord purgé(s)", len(rows))
    except Exception:
        log.exception("Erreur de maintenance")


# ======================================================================================
# BOT
# ======================================================================================
class TribeBot(commands.Bot):
    async def setup_hook(self):
        if os.environ.get("AUTO_SYNC", "1") == "1":
            try:
                await self.tree.sync()
            except Exception:
                log.exception("Synchronisation des commandes impossible")
        monitor.start()
        maintenance.start()


intents = discord.Intents.default()      # aucun intent privilégié nécessaire
bot = TribeBot(command_prefix=commands.when_mentioned, intents=intents, help_command=None)


@monitor.before_loop
async def _bm():
    await bot.wait_until_ready()


@maintenance.before_loop
async def _bmt():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    log.info("%s connecté : %s sur %d serveur(s) Discord", BRAND, bot.user, len(bot.guilds))
    for guild in bot.guilds:
        await ensure_guild(guild.id)
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="/help"))


def site_page(name):
    """Adresse d'une page du site (ex. guide.html), à partir de SITE_URL ou de l'adresse de la console."""
    base = SITE_URL or (DASHBOARD_URL.rsplit("/", 1)[0] if DASHBOARD_URL else "")
    return f"{base.rstrip('/')}/{name}" if base else ""


def setup_guide(guild_id, lang, title="welcome_title"):
    """Tuto de démarrage : embed + boutons (site, tuto complet, support)."""
    lines = [tr(lang, "welcome_desc"), ""]
    if WEB_ENABLED:
        lines.append(tr(lang, "welcome_web"))
    lines.append(tr(lang, "welcome_dc"))
    embed = discord.Embed(title=tr(lang, title, brand=BRAND), description="\n".join(lines), color=discord.Color.blue())
    embed.set_footer(text=tr(lang, "welcome_footer"))
    view = discord.ui.View()
    if WEB_ENABLED:
        view.add_item(discord.ui.Button(label=tr(lang, "btn_web"), url=f"{DASHBOARD_URL}#g={guild_id}"))
    guide = site_page("guide.html" if lang == "en" else "guide.html?lang=fr")
    if guide:
        view.add_item(discord.ui.Button(label=tr(lang, "btn_guide"), url=guide))
    if SUPPORT_URL:
        view.add_item(discord.ui.Button(label=tr(lang, "btn_support"), url=SUPPORT_URL))
    return embed, view


@bot.event
async def on_guild_join(guild):
    await ensure_guild(guild.id)
    target = guild.system_channel if can_write(guild.system_channel) else next(
        (c for c in guild.text_channels if can_write(c)), None)
    if target:
        # Langue du message : celle déclarée par la communauté Discord si c'est du français, sinon DEFAULT_LANG.
        lang = "fr" if str(getattr(guild, "preferred_locale", "")).startswith("fr") else DEFAULT_LANG
        embed, view = setup_guide(guild.id, lang)
        await safe_send(target, embed=embed, view=view)


@bot.event
async def on_guild_remove(guild):
    await adb("UPDATE guilds SET left_at = ? WHERE guild_id = ?", (utcnow().isoformat(), guild.id))
    forget_guild(guild.id)


async def lang_of(guild_id):
    """Langue du serveur Discord (par défaut : DEFAULT_LANG)."""
    if not guild_id:
        return DEFAULT_LANG
    try:
        return (await get_guild(guild_id))["lang"]
    except Exception:
        return DEFAULT_LANG


@bot.tree.error
async def on_app_error(interaction: discord.Interaction, error):
    log.error("Erreur de commande /%s", getattr(interaction.command, "qualified_name", "?"), exc_info=error)
    msg = tr(await lang_of(interaction.guild_id), "err_generic")
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


class AdminGroup(app_commands.Group):
    """Groupe de commandes réservé aux membres ayant la permission « Gérer le serveur »."""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        perms = getattr(interaction.user, "guild_permissions", None)
        if perms and perms.manage_guild:
            return True
        await interaction.response.send_message(tr(await lang_of(interaction.guild_id), "err_admin"), ephemeral=True)
        return False


def admin_group(name, description):
    return AdminGroup(name=name, description=description, guild_only=True,
                      default_permissions=discord.Permissions(manage_guild=True))


# ======================================================================================
# ACTIONS DE CONFIGURATION (partagées entre les commandes Discord et la console web)
# Chaque action renvoie (ok, [(clé de texte, paramètres), ...]) ; render() met le texte en forme.
# ======================================================================================
USER_TEXT_KEYS = {"name", "p", "t"}                # paramètres venant d'un utilisateur : échappés pour Discord
TZ_NAMES = sorted(available_timezones())


def render(lang, parts, web=False):
    out = []
    for key, kw in parts:
        kw2 = {}
        for k, v in kw.items():
            if isinstance(v, discord.abc.GuildChannel):
                v = f"#{v.name}" if web else v.mention
            elif k in USER_TEXT_KEYS and not web:
                v = nm(v)
            kw2[k] = v
        out.append(tr(lang, key, **kw2))
    text = "".join(out)
    if web:                                          # pas de markdown Discord sur le site
        text = text.replace("**", "").replace("`", "").strip()
    return text


async def core_channels(guild, dashboard, history):
    g = await get_guild(guild.id)
    for ch in (dashboard, history):
        if ch is not None and not can_write(ch):
            return False, [("ch_perm", {"ch": ch})]
    if g.get("channel_id") != dashboard.id:        # nouveau salon : on repart de zéro côté affichage
        old = guild.get_channel(g.get("channel_id")) if g.get("channel_id") else None
        s = st(g)
        if old:
            for mid in s["roster_ids"] + ([s["dashboard_id"]] if s["dashboard_id"] else []):
                try:
                    await old.get_partial_message(mid).delete()
                except discord.HTTPException:
                    pass
        gstate.pop(guild.id, None)
        await adb("UPDATE guilds SET dashboard_id = NULL, roster_ids = NULL WHERE guild_id = ?", (guild.id,))
    await adb("UPDATE guilds SET channel_id = ?, history_channel_id = ? WHERE guild_id = ?",
              (dashboard.id, history.id if history else None, guild.id))
    return True, [("ch_dash", {"ch": dashboard}), ("ch_hist", {"ch": history}) if history else ("ch_nohist", {})]


async def core_timezone(gid, name):
    if name not in TZ_NAMES:
        return False, [("tz_unknown", {})]
    await get_guild(gid)
    await adb("UPDATE guilds SET tz = ? WHERE guild_id = ?", (name, gid))
    return True, [("tz_set", {"tz": name})]


async def core_language(gid, code):
    await get_guild(gid)
    await adb("UPDATE guilds SET lang = ? WHERE guild_id = ?", (code, gid))
    gstate.pop(gid, None)                            # le prochain relevé réécrit les messages dans la nouvelle langue
    return True, [("lang_set", {"name": LANGS[code]})]


async def core_add_server(gid, name, address):
    name = name.strip()
    g = await get_guild(gid)
    lang = g["lang"]
    rows = (await adb("SELECT name FROM servers WHERE guild_id = ?", (gid,), fetch="all")) or []
    limit = feat(g, "max_servers")
    if len(rows) >= limit:
        up = tr(lang, "add_up", n=PLANS["premium"]["max_servers"], url=PREMIUM_URL) if (g["plan"] != "premium" and not FREE_MODE) else ""
        return False, [("add_limit", {"limit": limit, "plan": plan_label(lang, g["plan"]), "up": up})]
    if any(r[0].lower() == name.lower() for r in rows):
        return False, [("add_exists", {"name": name})]
    try:
        ip, port = parse_address(address)
    except ValueError:
        return False, [("add_badaddr", {})]
    try:
        await asyncio.to_thread(resolve_public, ip)
    except (ValueError, OSError) as exc:
        return False, [("add_refused", {"err": exc})]
    await adb("INSERT INTO servers (guild_id, name, host, port, status) VALUES (?, ?, ?, ?, 'unknown') "
              "ON CONFLICT(guild_id, name) DO NOTHING", (gid, name, ip, port))
    test = await asyncio.to_thread(query_server, ip, port)
    if test["online"]:
        return True, [("add_ok", {"name": name, "c": len(test["players"]), "m": test["max_players"]})]
    return True, [("add_noresp", {"name": name})]


async def core_remove_server(gid, name):
    n = await adb("DELETE FROM servers WHERE guild_id = ? AND LOWER(name) = LOWER(?)", (gid, name.strip()), fetch="rowcount")
    if n:
        for key in [k for k in previous_players if k[0] == gid and k[1].lower() == name.strip().lower()]:
            forget_server(*key)
    return bool(n), [("rm_ok" if n else "rm_none", {"name": name})]


async def core_bl_add(gid, player):
    player = player.strip()
    g = await get_guild(gid)
    lang = g["lang"]
    row = await adb("SELECT COUNT(*) FROM blacklist WHERE guild_id = ?", (gid,), fetch="one")
    if row and row[0] >= feat(g, "max_blacklist"):
        up = tr(lang, "bl_up", n=PLANS["premium"]["max_blacklist"], url=PREMIUM_URL) if (g["plan"] != "premium" and not FREE_MODE) else ""
        return False, [("bl_full", {"n": feat(g, "max_blacklist"), "up": up})]
    await adb("INSERT INTO blacklist (guild_id, player_name) VALUES (?, ?) ON CONFLICT(guild_id, player_name) DO NOTHING",
              (gid, player))
    return True, [("bl_added", {"p": player})]


async def core_bl_remove(gid, player):
    n = await adb("DELETE FROM blacklist WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                  (gid, player.strip()), fetch="rowcount")
    return bool(n), [("bl_removed" if n else "bl_notin", {"p": player})]


async def core_tribe_set(gid, player, tribe):
    row = await adb("SELECT COUNT(*) FROM player_tribes WHERE guild_id = ?", (gid,), fetch="one")
    exists = await adb("SELECT 1 FROM player_tribes WHERE guild_id = ? AND player_name = ?", (gid, player.strip()), fetch="one")
    if row and row[0] >= MAX_LIST and not exists:
        return False, [("tr_limit", {"n": MAX_LIST})]
    await adb("INSERT INTO player_tribes (guild_id, player_name, tribe_name) VALUES (?, ?, ?) "
              "ON CONFLICT(guild_id, player_name) DO UPDATE SET tribe_name = excluded.tribe_name",
              (gid, player.strip(), tribe.strip()))
    return True, [("tr_set", {"p": player, "t": tribe})]


async def core_tribe_remove(gid, player):
    n = await adb("DELETE FROM player_tribes WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                  (gid, player.strip()), fetch="rowcount")
    return bool(n), [("tr_removed" if n else "tr_none", {"p": player})]


# ======================================================================================
# COMMANDES : /config
# ======================================================================================
config_group = admin_group("config", "Configure the bot for this Discord server")


async def ac_my_servers(interaction: discord.Interaction, current: str):
    rows = await adb("SELECT name FROM servers WHERE guild_id = ?", (interaction.guild_id,), fetch="all")
    cur = current.lower()
    return [app_commands.Choice(name=r[0][:100], value=r[0][:100]) for r in rows or [] if cur in r[0].lower()][:25]


async def ac_tz(interaction: discord.Interaction, current: str):
    cur = current.lower().replace(" ", "_")
    return [app_commands.Choice(name=z, value=z) for z in TZ_NAMES if cur in z.lower()][:25]


@config_group.command(name="channels", description="Choose the dashboard channel and the history channel.")
@app_commands.describe(dashboard="Dashboard channel (players online)",
                       history="Channel for connections, alerts and recaps (optional)")
async def cfg_channels(interaction: discord.Interaction, dashboard: discord.TextChannel,
                       history: discord.TextChannel = None):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_channels(interaction.guild, dashboard, history)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@config_group.command(name="timezone", description="Set the time zone (displayed times and recaps).")
@app_commands.describe(name="E.g. Europe/Paris, America/New_York, Europe/London")
@app_commands.autocomplete(name=ac_tz)
async def cfg_timezone(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_timezone(interaction.guild_id, name)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@config_group.command(name="language", description="Choose the bot's language for this Discord server.")
@app_commands.describe(language="English or Français")
@app_commands.choices(language=[app_commands.Choice(name=n, value=c) for c, n in LANGS.items()])
async def cfg_language(interaction: discord.Interaction, language: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True)
    _, parts = await core_language(interaction.guild_id, language.value)
    await interaction.followup.send(render(language.value, parts), ephemeral=True)


@config_group.command(name="add-server", description="Add a game server to monitor.")
@app_commands.describe(name="Display name (e.g. Ragnarok)",
                       address="IP:query port, e.g. 151.242.127.127:27018")
async def cfg_add(interaction: discord.Interaction, name: app_commands.Range[str, 1, 32],
                  address: app_commands.Range[str, 5, 260]):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_add_server(interaction.guild_id, name, address)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@config_group.command(name="remove-server", description="Remove a game server.")
@app_commands.describe(name="Server name")
@app_commands.autocomplete(name=ac_my_servers)
async def cfg_remove(interaction: discord.Interaction, name: str):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_remove_server(interaction.guild_id, name)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@config_group.command(name="status", description="Show the configuration and the state of your servers.")
async def cfg_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    g = await get_guild(interaction.guild_id)
    lang = g["lang"]
    guild = interaction.guild
    rows = (await adb("SELECT name, host, port, status FROM servers WHERE guild_id = ? ORDER BY name",
                      (g["guild_id"],), fetch="all")) or []
    plan = PLANS.get(g["plan"], PLANS["free"])

    def chan(cid):
        ch = guild.get_channel(cid) if cid else None
        if not ch:
            return tr(lang, "status_undefined")
        return f"{ch.mention} " + ("✅" if can_write(ch) else tr(lang, "status_noperm"))

    icon = {ST_ON: "🟢", ST_OFF: "🔴"}
    lines = [f"{icon.get(norm_status(st_), '⚪')} **{nm(n)}** — `{h}:{p}`" for n, h, p, st_ in rows] \
        or [tr(lang, "status_noservers")]
    embed = discord.Embed(title=tr(lang, "status_title", brand=BRAND), color=discord.Color.blurple())
    embed.add_field(name=tr(lang, "status_plan"),
                    value=tr(lang, "status_plan_val", plan=plan_label(lang, g["plan"]), n=len(rows),
                             max=plan["max_servers"]), inline=False)
    embed.add_field(name=tr(lang, "status_channels"),
                    value=tr(lang, "status_channels_val", d=chan(g["channel_id"]), h=chan(g["history_channel_id"])),
                    inline=False)
    embed.add_field(name=tr(lang, "status_tz"), value=g["tz"] or DEFAULT_TZ, inline=False)
    embed.add_field(name=tr(lang, "status_lang"), value=LANGS.get(lang, lang), inline=False)
    embed.add_field(name=tr(lang, "status_servers"), value="\n".join(lines)[:1024], inline=False)
    if g["plan"] != "premium" and not FREE_MODE:
        embed.add_field(name=tr(lang, "status_premium"),
                        value=tr(lang, "status_premium_val", url=PREMIUM_URL).strip(), inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(config_group)

# ======================================================================================
# COMMANDES : /blacklist et /tribe
# ======================================================================================
blacklist_group = admin_group("blacklist", "Manage the list of watched players")


@blacklist_group.command(name="add", description="Add a player to the blacklist (alert when they connect).")
@app_commands.describe(player="Exact player name")
async def bl_add(interaction: discord.Interaction, player: app_commands.Range[str, 1, 64]):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_bl_add(interaction.guild_id, player)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@blacklist_group.command(name="remove", description="Remove a player from the blacklist.")
@app_commands.describe(player="Player name")
async def bl_remove(interaction: discord.Interaction, player: str):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_bl_remove(interaction.guild_id, player)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@blacklist_group.command(name="list", description="Show the blacklist.")
async def bl_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    rows = (await adb("SELECT player_name FROM blacklist WHERE guild_id = ? ORDER BY player_name",
                      (interaction.guild_id,), fetch="all")) or []
    if not rows:
        await interaction.followup.send(tr(lang, "bl_empty"), ephemeral=True)
        return
    desc = "\n".join(f"🟥 `{r[0].replace('`', '')}`" for r in rows)[:4000]
    await interaction.followup.send(embed=discord.Embed(title=tr(lang, "bl_title"), description=desc,
                                                        color=discord.Color.dark_red()), ephemeral=True)


bot.tree.add_command(blacklist_group)

tribe_group = admin_group("tribe", "Link players to tribes")


@tribe_group.command(name="set", description="Link a player to a tribe.")
@app_commands.describe(player="Player name", tribe="Tribe name")
async def tr_set(interaction: discord.Interaction, player: app_commands.Range[str, 1, 64],
                 tribe: app_commands.Range[str, 1, 32]):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_tribe_set(interaction.guild_id, player, tribe)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@tribe_group.command(name="remove", description="Remove a player's tribe affiliation.")
@app_commands.describe(player="Player name")
async def tr_remove(interaction: discord.Interaction, player: str):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    _, parts = await core_tribe_remove(interaction.guild_id, player)
    await interaction.followup.send(render(lang, parts), ephemeral=True)


@tribe_group.command(name="list", description="Show tribes and their members.")
async def tr_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    rows = (await adb("SELECT tribe_name, player_name FROM player_tribes WHERE guild_id = ? ORDER BY tribe_name",
                      (interaction.guild_id,), fetch="all")) or []
    if not rows:
        await interaction.followup.send(tr(lang, "tr_empty"), ephemeral=True)
        return
    grouped = {}
    for t, p in rows:
        grouped.setdefault(t, []).append(p)
    embed = discord.Embed(title=tr(lang, "tr_title"), color=discord.Color.orange())
    for t, members in list(grouped.items())[:25]:
        embed.add_field(name=f"[{t}]"[:256], value=", ".join(f"`{m.replace('`', '')}`" for m in members)[:1024], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(tribe_group)

# ======================================================================================
# COMMANDES : /data (confidentialité)
# ======================================================================================
data_group = admin_group("data", "Manage the data stored about players")


@data_group.command(name="delete-player", description="Delete all data of a player (history, playtime, tribe).")
@app_commands.describe(player="Exact player name")
async def data_player(interaction: discord.Interaction, player: str):
    await interaction.response.defer(ephemeral=True)
    gid, name = interaction.guild_id, player.strip()
    lang = await lang_of(gid)
    total = 0
    for table in ("history", "playtime", "player_tribes"):
        total += await adb(f"DELETE FROM {table} WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                           (gid, name), fetch="rowcount") or 0
    await interaction.followup.send(tr(lang, "data_deleted", p=nm(name), n=total), ephemeral=True)


@data_group.command(name="reset", description="Erase all history and playtime of this Discord server.")
@app_commands.describe(confirmation="Type DELETE to confirm")
async def data_reset(interaction: discord.Interaction, confirmation: str):
    await interaction.response.defer(ephemeral=True)
    lang = await lang_of(interaction.guild_id)
    if confirmation.strip().upper() not in {tr(c, "data_word") for c in LANGS}:
        await interaction.followup.send(tr(lang, "data_confirm", word=tr(lang, "data_word")), ephemeral=True)
        return
    for table in ("history", "playtime"):
        await adb(f"DELETE FROM {table} WHERE guild_id = ?", (interaction.guild_id,))
    await interaction.followup.send(tr(lang, "data_reset_ok"), ephemeral=True)


bot.tree.add_command(data_group)

# ======================================================================================
# COMMANDES : PUBLIQUES (/info /history /stats /help)
# ======================================================================================
async def ac_player(interaction: discord.Interaction, current: str):
    try:
        rows = await adb(f"SELECT DISTINCT player_name FROM history WHERE guild_id = ? AND {like_clause('player_name')} LIMIT 25",
                         (interaction.guild_id, like(current)), fetch="all")
        return [app_commands.Choice(name=r[0][:100], value=r[0][:100]) for r in rows or []]
    except Exception:
        return []


async def ac_map(interaction: discord.Interaction, current: str):
    return await ac_my_servers(interaction, current)


@bot.tree.command(name="info", description="Total playtime and playtime per map of a player.")
@app_commands.guild_only()
@app_commands.describe(player="The player you're looking for")
@app_commands.autocomplete(player=ac_player)
async def info_slash(interaction: discord.Interaction, player: str):
    g = await get_guild(interaction.guild_id)
    lang = g["lang"]
    if g.get("history_channel_id") and interaction.channel_id != g["history_channel_id"]:
        await interaction.response.send_message(tr(lang, "use_channel", id=g["history_channel_id"]), ephemeral=True)
        return
    await interaction.response.defer()
    gid = interaction.guild_id
    best = await adb(f"SELECT player_name, SUM(seconds) AS t FROM playtime WHERE guild_id = ? AND {like_clause('player_name')} "
                     "GROUP BY player_name ORDER BY t DESC LIMIT 1", (gid, like(player)), fetch="one")
    if not best:
        await interaction.followup.send(tr(lang, "info_none", p=nm(player)))
        return
    name, total_sec = best
    rows = (await adb("SELECT map_name, SUM(seconds) FROM playtime WHERE guild_id = ? AND player_name = ? "
                      "GROUP BY map_name ORDER BY 2 DESC", (gid, name), fetch="all")) or []
    bl_rows = await adb("SELECT player_name FROM blacklist WHERE guild_id = ?", (gid,), fetch="all")
    tr_rows = await adb("SELECT player_name, tribe_name FROM player_tribes WHERE guild_id = ?", (gid,), fetch="all")
    blacklist = {r[0].lower() for r in bl_rows or []}
    tribes = {r[0].lower(): r[1] for r in tr_rows or []}
    is_b = is_blacklisted(name, blacklist)
    embed = discord.Embed(title=tr(lang, "info_title", bl="🟥 " if is_b else "", p=nm(name), tag=tribe_tag(name, tribes)),
                          color=discord.Color.dark_red() if is_b else discord.Color.blue())
    embed.add_field(name=tr(lang, "info_total"), value=f"**{fmt_duration(total_sec)}**", inline=False)
    embed.add_field(name=tr(lang, "info_maps"),
                    value="\n".join(f"• **{nm(m)}** : `{fmt_duration(x)}`" for m, x in rows if x)[:1024] or "—", inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="history", description="Last connection of a player on each map (or who played on a map).")
@app_commands.guild_only()
@app_commands.describe(player="Player name (or the start of it)", map_name="Map / server name")
@app_commands.rename(map_name="map")
@app_commands.autocomplete(player=ac_player, map_name=ac_map)
async def history_slash(interaction: discord.Interaction, player: str = None, map_name: str = None):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    g = await get_guild(gid)
    lang, tz = g["lang"], tz_of(g)
    bl_rows = await adb("SELECT player_name FROM blacklist WHERE guild_id = ?", (gid,), fetch="all")
    tr_rows = await adb("SELECT player_name, tribe_name FROM player_tribes WHERE guild_id = ?", (gid,), fetch="all")
    blacklist = {r[0].lower() for r in bl_rows or []}
    tribes = {r[0].lower(): r[1] for r in tr_rows or []}

    query = "SELECT player_name, map_name, last_seen FROM history WHERE guild_id = ?"
    params = [gid]
    if player:
        query += f" AND {like_clause('player_name')}"
        params.append(like(player.strip()))
    if map_name:
        query += f" AND {like_clause('map_name')}"
        params.append(like(map_name.strip()))
    query += " ORDER BY last_seen DESC LIMIT 400"
    rows = (await adb(query, tuple(params), fetch="all")) or []
    if not rows:
        await interaction.followup.send(tr(lang, "hist_none"), ephemeral=True)
        return

    now = datetime.now(tz)
    live = online_now(gid)

    def seen(p, m, ls):
        if (m, p) in live:
            return "🟢", tr(lang, "hist_online")
        dt = parse_ts(ls, tz)
        if not dt:
            return "▫️", str(ls)
        return "▫️", tr(lang, "hist_when", ago=fmt_ago((now - dt).total_seconds(), lang),
                        date=dt.strftime("%d %b"), time=dt.strftime("%H:%M"))

    def who(p):
        return f"{'🟥 ' if is_blacklisted(p, blacklist) else ''}**{nm(p)}**{tribe_tag(p, tribes)}"

    blocks = []
    if player or not map_name:
        by_player = {}
        for p, m, ls in rows:
            by_player.setdefault(p, []).append((m, ls))
        cap = 13 if len(by_player) <= 3 else 3
        for p, maps in by_player.items():
            lines = [who(p)]
            for m, ls in maps[:cap]:
                dot, txt = seen(p, m, ls)
                lines.append(f"└ {dot} {nm(m)} — {txt}")
            if len(maps) > cap:
                lines.append(tr(lang, "hist_more_maps", n=len(maps) - cap))
            blocks.append("\n".join(lines))
        count = tr(lang, "hist_n_players", n=len(by_player))
    else:
        by_map = {}
        for p, m, ls in rows:
            by_map.setdefault(m, []).append((p, ls))
        for m, plist in by_map.items():
            lines = [tr(lang, "hist_map_head", m=nm(m), n=len(plist))]
            for p, ls in plist[:12]:
                dot, txt = seen(p, m, ls)
                lines.append(f"└ {dot} {who(p)} — {txt}")
            if len(plist) > 12:
                lines.append(tr(lang, "hist_more", n=len(plist) - 12))
            blocks.append("\n".join(lines))
        count = tr(lang, "hist_n_maps", n=len(by_map))

    crit = " • ".join(x for x in (tr(lang, "hist_crit_player", p=nm(player)) if player else None,
                                  tr(lang, "hist_crit_map", m=nm(map_name)) if map_name else None) if x)
    embed = discord.Embed(title=tr(lang, "hist_title"),
                          description=f"*{crit or tr(lang, 'hist_crit_default')}* — {count}\n\n{pack_blocks(blocks, lang)}",
                          color=discord.Color.blue())
    embed.set_footer(text=tr(lang, "hist_footer", tz=tz.key))
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="stats", description="Statistics of this Discord server.")
@app_commands.guild_only()
async def stats_slash(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    lang = await lang_of(gid)
    row = await adb("SELECT (SELECT COUNT(DISTINCT player_name) FROM history WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM blacklist WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM player_tribes WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM servers WHERE guild_id = ?)", (gid, gid, gid, gid), fetch="one")
    u, b, t, s = row if row else (0, 0, 0, 0)
    embed = discord.Embed(title=tr(lang, "stats_title"), color=discord.Color.green())
    embed.add_field(name=tr(lang, "stats_unique"), value=str(u), inline=False)
    embed.add_field(name=tr(lang, "stats_servers"), value=str(s), inline=False)
    embed.add_field(name=tr(lang, "stats_bl"), value=str(b), inline=False)
    embed.add_field(name=tr(lang, "stats_tribes"), value=str(t), inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="help", description="List of commands and useful links.")
async def help_slash(interaction: discord.Interaction):
    lang = await lang_of(interaction.guild_id)
    embed = discord.Embed(title=tr(lang, "help_title", brand=BRAND), color=discord.Color.blue(),
                          description=tr(lang, "help_desc"))
    links = []
    if SITE_URL:
        links.append(f"[{tr(lang, 'help_site')}]({SITE_URL}) • [{tr(lang, 'help_privacy')}]({SITE_URL.rstrip('/')}/#confidentialite)")
    if SUPPORT_URL:
        links.append(f"[{tr(lang, 'help_support')}]({SUPPORT_URL})")
    if links:
        embed.add_field(name=tr(lang, "help_links"), value=" • ".join(links), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="setup", description="Step-by-step guide to set up the bot.")
@app_commands.guild_only()
async def setup_slash(interaction: discord.Interaction):
    embed, view = setup_guide(interaction.guild_id, await lang_of(interaction.guild_id), "setup_title")
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


# ======================================================================================
# COMMANDES DU PROPRIÉTAIRE (en mentionnant le bot : "@TribeSpy premium 123 premium")
# ======================================================================================
@bot.command(name="premium")
@commands.is_owner()
async def owner_premium(ctx, guild_id: int, plan: str = "premium"):
    """@Bot premium <Discord server id> <free|premium>"""
    if plan not in PLANS:
        await ctx.send(f"Unknown plan. Choices: {', '.join(PLANS)}")
        return
    await ensure_guild(guild_id)
    await adb("UPDATE guilds SET plan = ? WHERE guild_id = ?", (plan, guild_id))
    await ctx.send(f"✅ Server {guild_id} → plan **{plan}**.")


@bot.command(name="sync")
@commands.is_owner()
async def owner_sync(ctx):
    synced = await bot.tree.sync()
    await ctx.send(f"✅ {len(synced)} commands synced.")


@bot.command(name="bilan")
@commands.is_owner()
async def owner_bilan(ctx):
    rows = await adb("SELECT plan, COUNT(*) FROM guilds WHERE left_at IS NULL GROUP BY plan", fetch="all")
    n_srv = await adb("SELECT COUNT(*) FROM servers", fetch="one")
    await ctx.send(f"📈 Discord servers: {len(bot.guilds)} • by plan: {dict(rows or [])} • game servers tracked: {n_srv[0]}")


# ======================================================================================
# SERVEUR WEB : santé / keep-alive + API de la console de configuration du site
# ======================================================================================
# Variables d'environnement de la console web (toutes les trois sont nécessaires pour l'activer) :
#   DISCORD_CLIENT_SECRET : secret OAuth2 de l'application Discord (Developer Portal > OAuth2)
#   PUBLIC_URL            : adresse publique de CE bot (automatique sur Render), ex. https://tribespy.onrender.com
#   DASHBOARD_URL         : adresse de la page console du site (par défaut SITE_URL + /config.html)
CLIENT_SECRET = os.environ.get("DISCORD_CLIENT_SECRET", "")
# Sur Render, l'adresse publique est fournie automatiquement (RENDER_EXTERNAL_URL).
PUBLIC_URL = (os.environ.get("PUBLIC_URL") or os.environ.get("RENDER_EXTERNAL_URL", "")).rstrip("/")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL") or (SITE_URL.rstrip("/") + "/config.html" if SITE_URL else "")
WEB_ENABLED = bool(CLIENT_SECRET and PUBLIC_URL and DASHBOARD_URL)
WEB_SESSION_DAYS = 7
DISCORD_API = "https://discord.com/api/v10"
BOT_PERMISSIONS = 216064                         # mêmes droits que le bouton « Ajouter à Discord » du site
PERM_ADMIN, PERM_MANAGE_GUILD = 0x8, 0x20


def _origin(url):
    u = urlsplit(url)
    return f"{u.scheme}://{u.netloc}" if u.scheme and u.netloc else ""


CORS_ORIGINS = {o for o in [_origin(DASHBOARD_URL), _origin(SITE_URL)]
                + [_origin(x.strip()) for x in os.environ.get("CORS_ORIGINS", "").split(",")] if o}
_oauth_states = {}                               # state -> expiration (anti-CSRF du login)
_member_cache = {}                               # (gid, uid) -> (expiration, peut gérer ?)
_write_log = {}                                  # hash de session -> horodatages des dernières modifications


def client_id():
    return os.environ.get("DISCORD_CLIENT_ID") or str(bot.application_id or "")


def invite_url(guild_id=None):
    q = {"client_id": client_id(), "permissions": BOT_PERMISSIONS, "scope": "bot applications.commands"}
    if guild_id:
        q.update(guild_id=guild_id, disable_guild_select="true")
    return "https://discord.com/oauth2/authorize?" + urlencode(q)


def _hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def jerr(status, error, message=None):
    return web.json_response({"ok": False, "error": error, "message": message or error}, status=status)


@web.middleware
async def cors_mw(request, handler):
    origin = request.headers.get("Origin", "")
    if request.method == "OPTIONS":
        resp = web.Response(status=204)
    else:
        try:
            resp = await handler(request)
        except web.HTTPException as exc:          # converties en vraies réponses (redirections et erreurs JSON)
            resp = web.Response(status=exc.status, text=exc.text, content_type=exc.content_type)
            if isinstance(exc, web.HTTPRedirection):
                resp.headers["Location"] = str(exc.location)
        except Exception:
            log.exception("Erreur API %s %s", request.method, request.path)
            resp = jerr(500, "server_error")
    if origin and origin in CORS_ORIGINS:
        resp.headers["Access-Control-Allow-Origin"] = origin
        resp.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PATCH, DELETE, OPTIONS"
        resp.headers["Access-Control-Max-Age"] = "600"
        resp.headers["Vary"] = "Origin"
    return resp


# ---------- connexion Discord (OAuth2) ----------
async def auth_login(request):
    if not WEB_ENABLED:
        return web.Response(status=503, text="Web console disabled (DISCORD_CLIENT_SECRET / PUBLIC_URL / DASHBOARD_URL).")
    now = time.time()
    for k in [k for k, exp in _oauth_states.items() if exp < now]:
        _oauth_states.pop(k, None)
    state = secrets.token_urlsafe(24)
    _oauth_states[state] = now + 600
    q = {"client_id": client_id(), "response_type": "code", "scope": "identify guilds",
         "redirect_uri": PUBLIC_URL + "/auth/callback", "state": state, "prompt": "none"}
    raise web.HTTPFound("https://discord.com/oauth2/authorize?" + urlencode(q))


async def auth_callback(request):
    if not WEB_ENABLED:
        return web.Response(status=503, text="Web console disabled.")
    state, code = request.query.get("state", ""), request.query.get("code", "")
    if not code or _oauth_states.pop(state, 0) < time.time():
        raise web.HTTPFound(DASHBOARD_URL + "#error=login")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as http:
        data = {"client_id": client_id(), "client_secret": CLIENT_SECRET, "grant_type": "authorization_code",
                "code": code, "redirect_uri": PUBLIC_URL + "/auth/callback"}
        async with http.post(DISCORD_API + "/oauth2/token", data=data) as r:
            if r.status != 200:
                log.warning("OAuth : échange du code refusé (%s)", r.status)
                raise web.HTTPFound(DASHBOARD_URL + "#error=login")
            access = (await r.json())["access_token"]
        headers = {"Authorization": f"Bearer {access}"}
        async with http.get(DISCORD_API + "/users/@me", headers=headers) as r:
            user = await r.json()
        async with http.get(DISCORD_API + "/users/@me/guilds", headers=headers) as r:
            guilds = await r.json() if r.status == 200 else []
    # On ne garde que les serveurs que la personne peut gérer ; le jeton Discord n'est pas conservé.
    manageable = [{"id": g["id"], "name": g.get("name", ""), "icon": g.get("icon")} for g in guilds
                  if g.get("owner") or int(g.get("permissions", 0)) & (PERM_ADMIN | PERM_MANAGE_GUILD)]
    token = secrets.token_urlsafe(32)
    expires = (utcnow() + timedelta(days=WEB_SESSION_DAYS)).isoformat()
    await adb("INSERT INTO web_sessions (token_hash, user_id, username, avatar, guilds, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
              (_hash(token), int(user["id"]), user.get("global_name") or user.get("username", ""), user.get("avatar"),
               json.dumps(manageable), expires))
    raise web.HTTPFound(DASHBOARD_URL + "#token=" + token)


async def session_of(request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise web.HTTPUnauthorized(text='{"ok":false,"error":"auth"}', content_type="application/json")
    h = _hash(auth[7:].strip())
    row = await adb("SELECT user_id, username, avatar, guilds, expires_at FROM web_sessions WHERE token_hash = ?",
                    (h,), fetch="one")
    if not row or row[4] < utcnow().isoformat():
        raise web.HTTPUnauthorized(text='{"ok":false,"error":"auth"}', content_type="application/json")
    return {"hash": h, "user_id": int(row[0]), "username": row[1], "avatar": row[2], "guilds": json.loads(row[3] or "[]")}


async def can_manage(guild, user_id):
    """Vérifie à chaque requête (cache 60 s) que la personne a toujours « Gérer le serveur » sur ce Discord."""
    key, now = (guild.id, user_id), time.time()
    hit = _member_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    try:
        member = guild.get_member(user_id) or await guild.fetch_member(user_id)
        ok = member.guild_permissions.manage_guild
    except discord.NotFound:
        ok = False
    _member_cache[key] = (now + 60, ok)
    return ok


async def guild_access(request, write=False):
    sess = await session_of(request)
    try:
        gid = int(request.match_info["gid"])
    except ValueError:
        raise web.HTTPNotFound()
    guild = bot.get_guild(gid)
    if guild is None or not any(int(g["id"]) == gid for g in sess["guilds"]):
        raise web.HTTPNotFound(text='{"ok":false,"error":"guild"}', content_type="application/json")
    if not await can_manage(guild, sess["user_id"]):
        raise web.HTTPForbidden(text='{"ok":false,"error":"forbidden"}', content_type="application/json")
    if write:                                    # limite simple : 30 modifications par minute et par session
        now = time.time()
        recent = [t for t in _write_log.get(sess["hash"], []) if t > now - 60]
        if len(recent) >= 30:
            raise web.HTTPTooManyRequests(text='{"ok":false,"error":"rate"}', content_type="application/json")
        _write_log[sess["hash"]] = recent + [now]
    return sess, guild


def req_lang(request, fallback):
    lang = request.query.get("lang", "")
    return lang if lang in LANGS else fallback


async def body_of(request):
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"ok":false,"error":"json"}', content_type="application/json")
    if not isinstance(data, dict):
        raise web.HTTPBadRequest(text='{"ok":false,"error":"json"}', content_type="application/json")
    return data


def text_field(data, key, lo, hi):
    v = data.get(key)
    if not isinstance(v, str) or not (lo <= len(v.strip()) <= hi):
        raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": "field", "field": key}),
                                 content_type="application/json")
    return v.strip()


async def answer(request, guild, result):
    ok, parts = result
    lang = req_lang(request, (await get_guild(guild.id))["lang"])
    return web.json_response({"ok": ok, "message": render(lang, parts, web=True)}, status=200 if ok else 400)


# ---------- API ----------
async def api_config(request):
    return web.json_response({"enabled": WEB_ENABLED, "login_url": PUBLIC_URL + "/auth/login" if WEB_ENABLED else None,
                              "invite_url": invite_url(), "langs": LANGS})


async def api_me(request):
    sess = await session_of(request)
    guilds = []
    for g in sess["guilds"]:
        present = bot.get_guild(int(g["id"])) is not None
        guilds.append({**g, "bot": present, "invite_url": None if present else invite_url(g["id"])})
    guilds.sort(key=lambda g: (not g["bot"], g["name"].lower()))
    return web.json_response({"user": {"id": str(sess["user_id"]), "name": sess["username"], "avatar": sess["avatar"]},
                              "guilds": guilds})


async def api_logout(request):
    sess = await session_of(request)
    await adb("DELETE FROM web_sessions WHERE token_hash = ?", (sess["hash"],))
    return web.json_response({"ok": True})


async def api_guild(request):
    _, guild = await guild_access(request)
    gid = guild.id
    g = await get_guild(gid)
    servers = (await adb("SELECT name, host, port, status FROM servers WHERE guild_id = ? ORDER BY name", (gid,), fetch="all")) or []
    bl = (await adb("SELECT player_name FROM blacklist WHERE guild_id = ? ORDER BY player_name", (gid,), fetch="all")) or []
    tribes = (await adb("SELECT player_name, tribe_name FROM player_tribes WHERE guild_id = ? ORDER BY tribe_name, player_name",
                        (gid,), fetch="all")) or []
    channels = [{"id": str(c.id), "name": c.name, "category": c.category.name if c.category else None, "writable": can_write(c)}
                for c in sorted(guild.text_channels, key=lambda c: c.position)]
    return web.json_response({
        "guild": {"id": str(gid), "name": guild.name, "icon": guild.icon.url if guild.icon else None},
        "settings": {"channel_id": str(g["channel_id"]) if g["channel_id"] else None,
                     "history_channel_id": str(g["history_channel_id"]) if g["history_channel_id"] else None,
                     "tz": g["tz"] or DEFAULT_TZ, "lang": g["lang"]},
        "plan": {"name": g["plan"], "max_servers": feat(g, "max_servers"), "max_blacklist": feat(g, "max_blacklist"),
                 "max_tribes": MAX_LIST},
        "channels": channels,
        "servers": [{"name": n, "address": f"{h}:{p}", "status": norm_status(s_)} for n, h, p, s_ in servers],
        "blacklist": [r[0] for r in bl],
        "tribes": [{"player": p, "tribe": t} for p, t in tribes],
        "timezones": TZ_NAMES,
    })


async def api_settings(request):
    _, guild = await guild_access(request, write=True)
    data = await body_of(request)
    parts, ok = [], True

    def channel(key):
        v = data.get(key)
        if not v:
            return None
        ch = guild.get_channel(int(v)) if str(v).isdigit() else None
        if not isinstance(ch, discord.TextChannel):
            raise web.HTTPBadRequest(text=json.dumps({"ok": False, "error": "field", "field": key}),
                                     content_type="application/json")
        return ch

    if "channel_id" in data:
        dash = channel("channel_id")
        if dash is None:
            raise web.HTTPBadRequest(text='{"ok":false,"error":"field","field":"channel_id"}', content_type="application/json")
        r_ok, r_parts = await core_channels(guild, dash, channel("history_channel_id"))
        ok, parts = ok and r_ok, parts + r_parts
    if ok and data.get("tz"):
        r_ok, r_parts = await core_timezone(guild.id, str(data["tz"]))
        ok, parts = ok and r_ok, parts + [("sep", {})] * bool(parts) + r_parts
    if ok and data.get("lang"):
        if data["lang"] not in LANGS:
            raise web.HTTPBadRequest(text='{"ok":false,"error":"field","field":"lang"}', content_type="application/json")
        r_ok, r_parts = await core_language(guild.id, data["lang"])
        ok, parts = ok and r_ok, parts + [("sep", {})] * bool(parts) + r_parts
    return await answer(request, guild, (ok, parts))


async def api_add_server(request):
    _, guild = await guild_access(request, write=True)
    data = await body_of(request)
    return await answer(request, guild, await core_add_server(guild.id, text_field(data, "name", 1, 32),
                                                              text_field(data, "address", 5, 260)))


async def api_remove_server(request):
    _, guild = await guild_access(request, write=True)
    return await answer(request, guild, await core_remove_server(guild.id, request.query.get("name", "")))


async def api_bl_add(request):
    _, guild = await guild_access(request, write=True)
    data = await body_of(request)
    return await answer(request, guild, await core_bl_add(guild.id, text_field(data, "player", 1, 64)))


async def api_bl_remove(request):
    _, guild = await guild_access(request, write=True)
    return await answer(request, guild, await core_bl_remove(guild.id, request.query.get("player", "")))


async def api_tribe_set(request):
    _, guild = await guild_access(request, write=True)
    data = await body_of(request)
    return await answer(request, guild, await core_tribe_set(guild.id, text_field(data, "player", 1, 64),
                                                             text_field(data, "tribe", 1, 32)))


async def api_tribe_remove(request):
    _, guild = await guild_access(request, write=True)
    return await answer(request, guild, await core_tribe_remove(guild.id, request.query.get("player", "")))


async def handle_ping(request):
    return web.Response(text=f"{BRAND} actif")


def build_web_app():
    app = web.Application(middlewares=[cors_mw], client_max_size=64 * 1024)
    r = app.router
    r.add_get("/", handle_ping)
    r.add_get("/auth/login", auth_login)
    r.add_get("/auth/callback", auth_callback)
    r.add_get("/api/config", api_config)
    r.add_get("/api/me", api_me)
    r.add_post("/api/logout", api_logout)
    r.add_get("/api/guilds/{gid}", api_guild)
    r.add_patch("/api/guilds/{gid}/settings", api_settings)
    r.add_post("/api/guilds/{gid}/servers", api_add_server)
    r.add_delete("/api/guilds/{gid}/servers", api_remove_server)
    r.add_post("/api/guilds/{gid}/blacklist", api_bl_add)
    r.add_delete("/api/guilds/{gid}/blacklist", api_bl_remove)
    r.add_post("/api/guilds/{gid}/tribes", api_tribe_set)
    r.add_delete("/api/guilds/{gid}/tribes", api_tribe_remove)
    r.add_route("OPTIONS", "/{tail:.*}", handle_ping)    # pré-requêtes CORS (réponse faite par le middleware)
    return app


async def start_web_server():
    runner = web.AppRunner(build_web_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 10000))).start()
    log.info("Console web %s", "activée" if WEB_ENABLED else "désactivée (variables OAuth manquantes)")


LOGIN_RETRY_MIN = 300          # 1re attente si Discord bloque la connexion (secondes)
LOGIN_RETRY_MAX = 900          # attente maximale entre deux essais


async def wait_for_discord():
    """Attend que Discord accepte à nouveau la connexion du bot.

    Une erreur 429 « global rate limit » au démarrage veut dire que l'adresse IP de l'hébergeur est
    bloquée temporairement. Relancer le bot en boucle prolonge le blocage : on patiente à la place,
    avec une seule requête de contrôle par essai.
    """
    delay = LOGIN_RETRY_MIN
    while True:
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
                async with s.get("https://discord.com/api/v10/users/@me",
                                 headers={"Authorization": f"Bot {TOKEN}"}) as r:
                    if r.status != 429:
                        return
                    try:
                        retry = float(r.headers.get("Retry-After") or 0)
                    except ValueError:
                        retry = 0
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("Discord injoignable (%s), nouvel essai dans 1 min", e)
            await asyncio.sleep(60)
            continue
        wait = min(max(delay, retry), 3600)
        log.warning("Discord bloque temporairement la connexion (429). Nouvel essai dans %d min.",
                    round(wait / 60))
        await asyncio.sleep(wait)
        delay = min(delay * 2, LOGIN_RETRY_MAX)


async def main():
    await start_web_server()          # le port reste ouvert même si Discord fait patienter le bot
    await wait_for_discord()
    try:
        async with bot:
            await bot.start(TOKEN)
    except discord.HTTPException as e:
        if e.status != 429:
            raise
        log.warning("Discord bloque temporairement la connexion (429). Redémarrage dans %d min.",
                    LOGIN_RETRY_MIN // 60)
        await asyncio.sleep(LOGIN_RETRY_MIN)      # évite que l'hébergeur relance le bot en boucle
        raise


if __name__ == "__main__":
    if not TOKEN:
        print("CRITICAL: DISCORD_TOKEN manquant !")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            pass
