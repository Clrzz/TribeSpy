"""
TribeSpy — bot Discord public (multi-serveurs) de suivi de serveurs de jeu compatibles Steam A2S.

* Chaque serveur Discord a sa propre configuration, ses propres serveurs de jeu et ses propres données.
* Aucun intent privilégié requis (pas de "message content").
* Lancement : python bot.py   (variables d'environnement : voir README.md)
"""
import asyncio
import ipaddress
import logging
import os
import re
import socket
import sqlite3
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, available_timezones

import a2s
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
PLANS = {
    "free": {"label": "Gratuit", "max_servers": int(os.environ.get("FREE_MAX_SERVERS", "3")),
             "max_blacklist": 10, "roster": False, "summaries": False, "raid": False},
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
        "last_weekly TEXT, created_at TEXT, left_at TEXT)",
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
    ]
    if IS_PG:
        db_execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
        cur_schema = db_execute("SELECT current_schema()", fetch="one")[0]
        if cur_schema != DB_SCHEMA:             # vérifié AVANT de créer les tables, pour ne rien mélanger
            raise SystemExit(f"Schéma actif « {cur_schema} » au lieu de « {DB_SCHEMA} » : utilise le Session pooler (port 5432).")
    for q in queries:
        db_execute(q)
    if IS_PG:
        # Sécurité (Supabase expose les tables par une API publique) : RLS activé sans règle = accès API refusé.
        # Le bot se connecte avec le propriétaire des tables, qui n'est pas concerné.
        for table in ("guilds", "servers", "blacklist", "player_tribes", "playtime", "history"):
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
              "last_daily, last_weekly")


def row_to_guild(r):
    return dict(zip(GUILD_COLS.split(", "), r))


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


def fmt_ago(secs):
    secs = max(int(secs), 0)
    if secs < 120:
        return "à l'instant"
    if secs < 3600:
        return f"il y a {secs // 60} min"
    if secs < 86400:
        return f"il y a {secs // 3600}h{(secs % 3600) // 60:02d}"
    return f"il y a {secs // 86400} j"


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


def pack_blocks(blocks, limit=3900):
    out, size = [], 0
    for i, blk in enumerate(blocks):
        if size + len(blk) + 2 > limit:
            out.append(f"… et {len(blocks) - i} autre(s) — précise ta recherche")
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


def resolve_public(host):
    """Résout l'hôte en IPv4 et refuse toute adresse non publique (réseau local, loopback, etc.)."""
    if not HOST_RE.match(host):
        raise ValueError("adresse invalide")
    infos = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_DGRAM)
    ips = sorted({i[4][0] for i in infos})
    if not ips:
        raise ValueError("hôte introuvable")
    for ip in ips:
        if not ipaddress.ip_address(ip).is_global:
            raise ValueError("adresse non publique refusée")
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
    gid = g["guild_id"]
    now = now_utc.astimezone(tz_of(g))
    today, time_str = now.strftime("%Y-%m-%d"), now.strftime("%H:%M")
    now_iso = now_utc.isoformat()
    views, feed, alerts, changed = [], [], [], False

    for s in servers:
        name, key = s["name"], (gid, s["name"])
        res = results[(s["host"], s["port"])]
        prev_status = s["status"] or "Inconnu"

        if res["online"]:
            fail_streak[key] = 0
            new_status = "En ligne"
            last_max[key] = res["max_players"]
        else:
            fail_streak[key] = fail_streak.get(key, 0) + 1
            new_status = "Hors ligne" if fail_streak[key] >= FAIL_THRESHOLD else prev_status

        if prev_status == "En ligne" and new_status == "Hors ligne":
            alerts.append(("@here ⚠️ Panne détectée sur un serveur !", discord.Embed(
                title="🚨 ALERTE CRASH SERVEUR !",
                description=f"Le serveur **{nm(name)}** vient de passer **HORS LIGNE** !",
                color=discord.Color.red()), True))
        elif prev_status == "Hors ligne" and new_status == "En ligne":
            alerts.append((None, discord.Embed(
                title="✅ Serveur de retour",
                description=f"Le serveur **{nm(name)}** est de nouveau **EN LIGNE**.",
                color=discord.Color.green()), False))
        if new_status != prev_status:
            out["status"].append((new_status, gid, name))
            s["status"] = new_status

        if res["online"]:
            views.append({"name": name, "online": True, "players": res["players"], "max": res["max_players"]})
        elif new_status == "En ligne":            # échec isolé : on garde l'affichage précédent
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
                alerts.append((f"🚨 **ALERTE BLACKLIST** 🚨 : Le joueur 🟥 **{nm(p)}**{tag} "
                               f"vient de se connecter sur **{nm(name)}** !", None, False))
            else:
                feed.append(f"🟢 `[{time_str}]` **{nm(p)}**{tag} a rejoint **{nm(name)}**")

        for p in sorted(left):
            start = session_starts.pop((gid, name, p), None)
            sess = f" *(Session : {fmt_duration((now - start).total_seconds())})*" if start else ""
            pre = "🟥 " if is_blacklisted(p, blacklist) else ""
            feed.append(f"🔴 `[{time_str}]` {pre}**{nm(p)}**{tribe_tag(p, tribes)} a quitté **{nm(name)}**{sess}")

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
                noms = ", ".join(f"🟥 **{nm(p)}**" for p in sorted(players))
                alerts.append(("@here 🛡️ PRÉPAREZ LES DÉFENSES !", discord.Embed(
                    title="🚨 ALERTE ROUGE : ATTAQUE / RAID IMMINENT ?",
                    description=f"**{len(players)} joueurs blacklistés** repérés en simultané sur "
                                f"**{nm(map_name)}** !\n\nJoueurs : {noms}",
                    color=discord.Color.dark_magenta()), True))
                lst = [c for c in lst if c["map"] != map_name]
        recent_bl[gid] = lst
    else:
        recent_bl[gid] = []

    return {"now": now, "views": views, "feed": feed, "alerts": alerts, "changed": changed}


# ======================================================================================
# AFFICHAGE : DASHBOARD
# ======================================================================================
def build_dashboard_embeds(views, blacklist, tribes):
    total = sum(len(v["players"]) for v in views if v["online"])
    blocks = []
    for v in views:
        if not v["online"]:
            blocks.append(f"🗺️ **{nm(v['name'])}**\n🔴 **Hors ligne**")
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
            plist = "Aucun joueur"
        blocks.append(f"🗺️ **{nm(v['name'])}**\n🟢 **{len(v['players'])}/{v['max']}** — {plist}")

    header = f"🎮 **Joueurs en ligne**\n👤 **Total : {total} joueur(s)**\n------------------------------------"
    pages, buf = [], header
    for i, blk in enumerate(blocks):
        if len(buf) + len(blk) + 1 > 2800:
            pages.append(buf)
            buf = ""
            if len(pages) == 2:                    # 6000 caractères max par message Discord
                buf = f"… et {len(blocks) - i} autre(s) serveur(s)"
                break
        buf += ("\n" if buf else "") + blk
    pages.append(buf)
    embeds = [discord.Embed(description=t, color=discord.Color.blue()) for t in pages]
    embeds[-1].set_footer(text=f"{BRAND} • actualisation toutes les {TICK}s • 🟥 = Blacklist • [Tribu]")
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
            log.warning("Édition du dashboard impossible (%s) : %s", gid, exc)
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


async def build_roster_embeds(gid, tz, blacklist, tribes, online):
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
    header = (f"📋 **Joueurs du cluster** — {len(active)} joueur(s) · 🟢 {n_online} en ligne\n"
              f"*Triés par temps de jeu • 2 maps les plus jouées*\n------------------------------------")
    if not lines:
        lines = ["Aucun joueur actif."]
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
        pages[-1] += f"\n… et {hidden} autre(s)"
    embeds = [discord.Embed(description=t, color=discord.Color.purple()) for t in pages]
    embeds[-1].set_footer(text=f"Inactifs depuis +{INACTIVE_DAYS} jours retirés automatiquement • 🟥 = Blacklist • ▫️ = hors ligne")
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
    gid, today = g["guild_id"], now.strftime("%Y-%m-%d")
    rows = (await adb("SELECT player_name, map_name, seconds FROM playtime WHERE guild_id = ? AND date = ?",
                      (gid, today), fetch="all")) or []
    active = {}
    for player, map_name, sec in rows:
        info = active.setdefault(player, {"servers": set(), "seconds": 0})
        info["servers"].add(map_name)
        info["seconds"] += sec or 0
    detected = [p for p in active if is_blacklisted(p, blacklist)]
    embed = discord.Embed(title="🌙 Bilan quotidien & temps de jeu",
                          description=f"Rapport d'activité du **{today}**", color=discord.Color.gold())
    embed.add_field(name="👥 Joueurs uniques actifs aujourd'hui", value=str(len(active)), inline=False)
    embed.add_field(name="🚨 Joueurs de la blacklist repérés aujourd'hui",
                    value="\n".join(f"🟥 **{nm(p)}**{tribe_tag(p, tribes)}" for p in detected)[:1024]
                    or "Aucun joueur blacklisté n'a été aperçu.", inline=False)
    if active:
        top = sorted(active.items(), key=lambda x: x[1]["seconds"], reverse=True)[:15]
        lines = []
        for p, info in top:
            pre = "🟥 " if is_blacklisted(p, blacklist) else "• "
            lines.append(f"{pre}**{nm(p)}**{tribe_tag(p, tribes)} — ⏱️ `{fmt_duration(info['seconds'])}` "
                         f"(*{nm(', '.join(sorted(info['servers'])))}*)")
        embed.add_field(name="⏱️ Temps de jeu de la journée", value="\n".join(lines)[:1024], inline=False)
    embed.set_footer(text=f"{BRAND} • bilan automatique de 23:00")
    await safe_send(hchan, embed=embed)


async def send_weekly(g, hchan, now):
    since = (now - timedelta(days=6)).strftime("%Y-%m-%d")
    rows = (await adb("SELECT player_name, SUM(seconds) AS total FROM playtime WHERE guild_id = ? AND date >= ? "
                      "GROUP BY player_name ORDER BY total DESC LIMIT 3", (g["guild_id"], since), fetch="all")) or []
    if not rows:
        return
    medals = ["🥇", "🥈", "🥉"]
    podium = "\n".join(f"{medals[i]} **{nm(n)}** — `{fmt_duration(t)}`" for i, (n, t) in enumerate(rows))
    await safe_send(hchan, embed=discord.Embed(
        title="👑 LE NO-LIFE DE LA SEMAINE",
        description=f"Félicitations à **{nm(rows[0][0])}** qui remporte la palme du tryhard !\n\n{podium}",
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
                embeds = await build_roster_embeds(g["guild_id"], tz_of(g), blacklist, tribes, online)
                await update_roster(g, channel, embeds)
            except Exception:
                log.exception("Erreur fiche joueurs (%s)", g["guild_id"])
        await update_dashboard(g, channel, build_dashboard_embeds(plan["views"], blacklist, tribes))

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
        await start_web_server()
        if os.environ.get("AUTO_SYNC", "1") == "1":
            try:
                await self.tree.sync()
            except discord.HTTPException:
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
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="/aide"))


@bot.event
async def on_guild_join(guild):
    await ensure_guild(guild.id)
    target = guild.system_channel if can_write(guild.system_channel) else next(
        (c for c in guild.text_channels if can_write(c)), None)
    if target:
        await safe_send(target, embed=discord.Embed(
            title=f"👋 Merci d'avoir ajouté {BRAND} !",
            description=("Pour démarrer (réservé aux membres avec la permission **Gérer le serveur**) :\n"
                         "1️⃣ `/config salons` — choisis le salon du tableau de bord\n"
                         "2️⃣ `/config serveur-ajouter` — ajoute ton serveur de jeu (IP + port de requête)\n"
                         "3️⃣ `/config fuseau` — règle ton fuseau horaire\n\n"
                         "Tape `/aide` pour voir toutes les commandes."),
            color=discord.Color.blue()))


@bot.event
async def on_guild_remove(guild):
    await adb("UPDATE guilds SET left_at = ? WHERE guild_id = ?", (utcnow().isoformat(), guild.id))
    forget_guild(guild.id)


@bot.tree.error
async def on_app_error(interaction: discord.Interaction, error):
    log.error("Erreur de commande /%s", getattr(interaction.command, "qualified_name", "?"), exc_info=error)
    msg = "❌ Une erreur est survenue. Réessaie dans un instant."
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
        await interaction.response.send_message(
            "⛔ Réservé aux membres ayant la permission « Gérer le serveur ».", ephemeral=True)
        return False


def admin_group(name, description):
    return AdminGroup(name=name, description=description, guild_only=True,
                      default_permissions=discord.Permissions(manage_guild=True))


# ======================================================================================
# COMMANDES : /config
# ======================================================================================
config_group = admin_group("config", "Configurer le bot pour ce serveur Discord")
TZ_NAMES = sorted(available_timezones())


async def ac_my_servers(interaction: discord.Interaction, current: str):
    rows = await adb("SELECT name FROM servers WHERE guild_id = ?", (interaction.guild_id,), fetch="all")
    cur = current.lower()
    return [app_commands.Choice(name=r[0][:100], value=r[0][:100]) for r in rows or [] if cur in r[0].lower()][:25]


async def ac_tz(interaction: discord.Interaction, current: str):
    cur = current.lower().replace(" ", "_")
    return [app_commands.Choice(name=z, value=z) for z in TZ_NAMES if cur in z.lower()][:25]


@config_group.command(name="salons", description="Choisir le salon du tableau de bord et celui de l'historique.")
@app_commands.describe(dashboard="Salon du tableau de bord (joueurs en ligne)",
                       historique="Salon des connexions, alertes et bilans (optionnel)")
async def cfg_salons(interaction: discord.Interaction, dashboard: discord.TextChannel,
                     historique: discord.TextChannel = None):
    await interaction.response.defer(ephemeral=True)
    me = interaction.guild.me
    for ch in (dashboard, historique):
        if ch is None:
            continue
        p = ch.permissions_for(me)
        if not (p.view_channel and p.send_messages and p.embed_links):
            await interaction.followup.send(
                f"❌ Je n'ai pas les droits **Voir le salon**, **Envoyer des messages** et **Intégrer des liens** "
                f"dans {ch.mention}.", ephemeral=True)
            return
    g = await get_guild(interaction.guild_id)
    if g.get("channel_id") != dashboard.id:        # nouveau salon : on repart de zéro côté affichage
        old = interaction.guild.get_channel(g.get("channel_id")) if g.get("channel_id") else None
        s = st(g)
        if old:
            for mid in s["roster_ids"] + ([s["dashboard_id"]] if s["dashboard_id"] else []):
                try:
                    await old.get_partial_message(mid).delete()
                except discord.HTTPException:
                    pass
        gstate.pop(interaction.guild_id, None)
        await adb("UPDATE guilds SET dashboard_id = NULL, roster_ids = NULL WHERE guild_id = ?",
                  (interaction.guild_id,))
    await adb("UPDATE guilds SET channel_id = ?, history_channel_id = ? WHERE guild_id = ?",
              (dashboard.id, historique.id if historique else None, interaction.guild_id))
    txt = f"✅ Tableau de bord : {dashboard.mention}"
    txt += f"\n✅ Historique : {historique.mention}" if historique else "\nℹ️ Pas de salon d'historique (alertes et connexions désactivées)."
    await interaction.followup.send(txt, ephemeral=True)


@config_group.command(name="fuseau", description="Définir le fuseau horaire (heures affichées et bilans).")
@app_commands.describe(nom="Ex : Europe/Paris, America/Montreal, Europe/Brussels")
@app_commands.autocomplete(nom=ac_tz)
async def cfg_fuseau(interaction: discord.Interaction, nom: str):
    await interaction.response.defer(ephemeral=True)
    if nom not in TZ_NAMES:
        await interaction.followup.send("❌ Fuseau inconnu. Commence à taper (ex : `Europe/Par`) et choisis dans la liste.",
                                        ephemeral=True)
        return
    await adb("UPDATE guilds SET tz = ? WHERE guild_id = ?", (nom, interaction.guild_id))
    await interaction.followup.send(f"✅ Fuseau horaire réglé sur **{nom}**.", ephemeral=True)


@config_group.command(name="serveur-ajouter", description="Ajouter un serveur de jeu à surveiller.")
@app_commands.describe(nom="Nom affiché (ex : Ragnarok)", ip="IP ou nom de domaine du serveur",
                       port="Port de REQUÊTE (query) du serveur, ex : 27015")
async def cfg_add(interaction: discord.Interaction, nom: app_commands.Range[str, 1, 32],
                  ip: app_commands.Range[str, 3, 253], port: app_commands.Range[int, 1024, 65535]):
    await interaction.response.defer(ephemeral=True)
    gid, nom, ip = interaction.guild_id, nom.strip(), ip.strip().lower()
    g = await get_guild(gid)
    rows = (await adb("SELECT name FROM servers WHERE guild_id = ?", (gid,), fetch="all")) or []
    limit = feat(g, "max_servers")
    if len(rows) >= limit:
        up = f"\n⭐ Passe en **Premium** pour aller jusqu'à {PLANS['premium']['max_servers']} serveurs : {PREMIUM_URL}" if g["plan"] != "premium" else ""
        await interaction.followup.send(f"❌ Limite atteinte ({limit} serveurs pour l'offre {PLANS[g['plan']]['label']}).{up}",
                                        ephemeral=True)
        return
    if any(r[0].lower() == nom.lower() for r in rows):
        await interaction.followup.send(f"❌ Un serveur nommé **{nm(nom)}** existe déjà.", ephemeral=True)
        return
    try:
        await asyncio.to_thread(resolve_public, ip)
    except (ValueError, OSError) as exc:
        await interaction.followup.send(f"❌ Adresse refusée : {exc}. Utilise l'IP publique ou le domaine de ton serveur.",
                                        ephemeral=True)
        return
    await adb("INSERT INTO servers (guild_id, name, host, port, status) VALUES (?, ?, ?, ?, 'Inconnu') "
              "ON CONFLICT(guild_id, name) DO NOTHING", (gid, nom, ip, port))
    test = await asyncio.to_thread(query_server, ip, port)
    if test["online"]:
        msg = f"✅ **{nm(nom)}** ajouté — serveur joignable ({len(test['players'])}/{test['max_players']} joueurs)."
    else:
        msg = (f"⚠️ **{nm(nom)}** ajouté, mais il ne répond pas pour l'instant. Vérifie que tu as bien mis le "
               f"**port de requête** (query port, souvent différent du port de jeu) et qu'il n'est pas bloqué par un pare-feu.")
    await interaction.followup.send(msg, ephemeral=True)


@config_group.command(name="serveur-retirer", description="Retirer un serveur de jeu.")
@app_commands.describe(nom="Nom du serveur")
@app_commands.autocomplete(nom=ac_my_servers)
async def cfg_remove(interaction: discord.Interaction, nom: str):
    await interaction.response.defer(ephemeral=True)
    n = await adb("DELETE FROM servers WHERE guild_id = ? AND LOWER(name) = LOWER(?)",
                  (interaction.guild_id, nom.strip()), fetch="rowcount")
    if n:
        for key in [k for k in previous_players if k[0] == interaction.guild_id and k[1].lower() == nom.strip().lower()]:
            forget_server(*key)
    await interaction.followup.send(f"🗑️ **{nm(nom)}** retiré." if n else f"❌ Aucun serveur nommé **{nm(nom)}**.",
                                    ephemeral=True)


@config_group.command(name="etat", description="Voir la configuration et l'état des serveurs.")
async def cfg_etat(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    g = await get_guild(interaction.guild_id)
    guild = interaction.guild
    rows = (await adb("SELECT name, host, port, status FROM servers WHERE guild_id = ? ORDER BY name",
                      (g["guild_id"],), fetch="all")) or []
    plan = PLANS.get(g["plan"], PLANS["free"])

    def chan(cid):
        ch = guild.get_channel(cid) if cid else None
        if not ch:
            return "❌ non défini"
        return f"{ch.mention} " + ("✅" if can_write(ch) else "⚠️ droits manquants")

    icon = {"En ligne": "🟢", "Hors ligne": "🔴"}
    lines = [f"{icon.get(st_, '⚪')} **{nm(n)}** — `{h}:{p}`" for n, h, p, st_ in rows] or ["*Aucun serveur configuré*"]
    embed = discord.Embed(title=f"⚙️ Configuration de {BRAND}", color=discord.Color.blurple())
    embed.add_field(name="Offre", value=f"**{plan['label']}** • {len(rows)}/{plan['max_servers']} serveurs", inline=False)
    embed.add_field(name="Salons", value=f"Dashboard : {chan(g['channel_id'])}\nHistorique : {chan(g['history_channel_id'])}", inline=False)
    embed.add_field(name="Fuseau horaire", value=g["tz"] or DEFAULT_TZ, inline=False)
    embed.add_field(name="Serveurs de jeu", value="\n".join(lines)[:1024], inline=False)
    if g["plan"] != "premium":
        embed.add_field(name="⭐ Premium", value="Fiche des joueurs, alertes raid, bilans quotidiens/hebdo, "
                                                 f"plus de serveurs. {PREMIUM_URL}".strip(), inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(config_group)

# ======================================================================================
# COMMANDES : /blacklist et /tribu
# ======================================================================================
blacklist_group = admin_group("blacklist", "Gérer la liste des joueurs surveillés")


@blacklist_group.command(name="ajouter", description="Ajoute un joueur à la blacklist (alerte à sa connexion).")
@app_commands.describe(joueur="Pseudo exact du joueur")
async def bl_add(interaction: discord.Interaction, joueur: app_commands.Range[str, 1, 64]):
    await interaction.response.defer(ephemeral=True)
    gid, joueur = interaction.guild_id, joueur.strip()
    g = await get_guild(gid)
    row = await adb("SELECT COUNT(*) FROM blacklist WHERE guild_id = ?", (gid,), fetch="one")
    if row and row[0] >= feat(g, "max_blacklist"):
        up = f"\n⭐ L'offre **Premium** permet jusqu'à {PLANS['premium']['max_blacklist']} joueurs : {PREMIUM_URL}" if g["plan"] != "premium" else ""
        await interaction.followup.send(f"❌ Blacklist pleine ({feat(g, 'max_blacklist')} joueurs max).{up}", ephemeral=True)
        return
    await adb("INSERT INTO blacklist (guild_id, player_name) VALUES (?, ?) ON CONFLICT(guild_id, player_name) DO NOTHING",
              (gid, joueur))
    await interaction.followup.send(f"✅ 🟥 **{nm(joueur)}** ajouté à la blacklist.", ephemeral=True)


@blacklist_group.command(name="retirer", description="Retire un joueur de la blacklist.")
@app_commands.describe(joueur="Pseudo du joueur")
async def bl_remove(interaction: discord.Interaction, joueur: str):
    await interaction.response.defer(ephemeral=True)
    n = await adb("DELETE FROM blacklist WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                  (interaction.guild_id, joueur.strip()), fetch="rowcount")
    await interaction.followup.send(f"🗑️ **{nm(joueur)}** retiré de la blacklist." if n
                                    else f"❌ **{nm(joueur)}** n'est pas dans la blacklist.", ephemeral=True)


@blacklist_group.command(name="liste", description="Affiche la blacklist.")
async def bl_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = (await adb("SELECT player_name FROM blacklist WHERE guild_id = ? ORDER BY player_name",
                      (interaction.guild_id,), fetch="all")) or []
    if not rows:
        await interaction.followup.send("📋 La blacklist est vide.", ephemeral=True)
        return
    desc = "\n".join(f"🟥 `{r[0].replace('`', '')}`" for r in rows)[:4000]
    await interaction.followup.send(embed=discord.Embed(title="🏴‍☠️ Joueurs blacklistés", description=desc,
                                                        color=discord.Color.dark_red()), ephemeral=True)


bot.tree.add_command(blacklist_group)

tribu_group = admin_group("tribu", "Associer des joueurs à des tribus")


@tribu_group.command(name="definir", description="Associe un joueur à une tribu.")
@app_commands.describe(joueur="Pseudo du joueur", tribu="Nom de la tribu")
async def tr_set(interaction: discord.Interaction, joueur: app_commands.Range[str, 1, 64],
                 tribu: app_commands.Range[str, 1, 32]):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    row = await adb("SELECT COUNT(*) FROM player_tribes WHERE guild_id = ?", (gid,), fetch="one")
    exists = await adb("SELECT 1 FROM player_tribes WHERE guild_id = ? AND player_name = ?", (gid, joueur.strip()), fetch="one")
    if row and row[0] >= MAX_LIST and not exists:
        await interaction.followup.send(f"❌ Limite de {MAX_LIST} affiliations atteinte.", ephemeral=True)
        return
    await adb("INSERT INTO player_tribes (guild_id, player_name, tribe_name) VALUES (?, ?, ?) "
              "ON CONFLICT(guild_id, player_name) DO UPDATE SET tribe_name = excluded.tribe_name",
              (gid, joueur.strip(), tribu.strip()))
    await interaction.followup.send(f"🏷️ **{nm(joueur)}** → tribu **[{nm(tribu)}]**.", ephemeral=True)


@tribu_group.command(name="retirer", description="Retire l'affiliation de tribu d'un joueur.")
@app_commands.describe(joueur="Pseudo du joueur")
async def tr_remove(interaction: discord.Interaction, joueur: str):
    await interaction.response.defer(ephemeral=True)
    n = await adb("DELETE FROM player_tribes WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                  (interaction.guild_id, joueur.strip()), fetch="rowcount")
    await interaction.followup.send(f"🗑️ Affiliation de **{nm(joueur)}** retirée." if n
                                    else f"❌ Aucune affiliation pour **{nm(joueur)}**.", ephemeral=True)


@tribu_group.command(name="liste", description="Affiche les tribus et leurs membres.")
async def tr_list(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    rows = (await adb("SELECT tribe_name, player_name FROM player_tribes WHERE guild_id = ? ORDER BY tribe_name",
                      (interaction.guild_id,), fetch="all")) or []
    if not rows:
        await interaction.followup.send("📋 Aucune affiliation enregistrée.", ephemeral=True)
        return
    grouped = {}
    for t, p in rows:
        grouped.setdefault(t, []).append(p)
    embed = discord.Embed(title="🛡️ Répertoire des tribus", color=discord.Color.orange())
    for t, members in list(grouped.items())[:25]:
        embed.add_field(name=f"[{t}]"[:256], value=", ".join(f"`{m.replace('`', '')}`" for m in members)[:1024], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


bot.tree.add_command(tribu_group)

# ======================================================================================
# COMMANDES : /donnees (confidentialité)
# ======================================================================================
data_group = admin_group("donnees", "Gérer les données enregistrées sur les joueurs")


@data_group.command(name="joueur-supprimer", description="Supprime toutes les données d'un joueur (historique, temps de jeu, tribu).")
@app_commands.describe(joueur="Pseudo exact du joueur")
async def data_player(interaction: discord.Interaction, joueur: str):
    await interaction.response.defer(ephemeral=True)
    gid, name = interaction.guild_id, joueur.strip()
    total = 0
    for table in ("history", "playtime", "player_tribes"):
        total += await adb(f"DELETE FROM {table} WHERE guild_id = ? AND LOWER(player_name) = LOWER(?)",
                           (gid, name), fetch="rowcount") or 0
    await interaction.followup.send(f"🗑️ Données de **{nm(name)}** supprimées ({total} enregistrement(s)). "
                                    f"Si le joueur est encore connecté, il sera de nouveau enregistré à la prochaine minute.",
                                    ephemeral=True)


@data_group.command(name="reinitialiser", description="Efface tout l'historique et les temps de jeu de ce serveur Discord.")
@app_commands.describe(confirmation="Tape SUPPRIMER pour confirmer")
async def data_reset(interaction: discord.Interaction, confirmation: str):
    await interaction.response.defer(ephemeral=True)
    if confirmation.strip() != "SUPPRIMER":
        await interaction.followup.send("❌ Tape exactement `SUPPRIMER` pour confirmer.", ephemeral=True)
        return
    for table in ("history", "playtime"):
        await adb(f"DELETE FROM {table} WHERE guild_id = ?", (interaction.guild_id,))
    await interaction.followup.send("🗑️ Historique et temps de jeu effacés.", ephemeral=True)


bot.tree.add_command(data_group)

# ======================================================================================
# COMMANDES : PUBLIQUES (/infos /historique /stats /aide)
# ======================================================================================
async def ac_joueur(interaction: discord.Interaction, current: str):
    try:
        rows = await adb(f"SELECT DISTINCT player_name FROM history WHERE guild_id = ? AND {like_clause('player_name')} LIMIT 25",
                         (interaction.guild_id, like(current)), fetch="all")
        return [app_commands.Choice(name=r[0][:100], value=r[0][:100]) for r in rows or []]
    except Exception:
        return []


async def ac_serveur(interaction: discord.Interaction, current: str):
    return await ac_my_servers(interaction, current)


@bot.tree.command(name="infos", description="Temps de jeu total et par map d'un joueur.")
@app_commands.guild_only()
@app_commands.describe(joueur="Le pseudo du joueur recherché")
@app_commands.autocomplete(joueur=ac_joueur)
async def infos_slash(interaction: discord.Interaction, joueur: str):
    g = await get_guild(interaction.guild_id)
    if g.get("history_channel_id") and interaction.channel_id != g["history_channel_id"]:
        await interaction.response.send_message(f"❌ Utilise cette commande dans <#{g['history_channel_id']}> !", ephemeral=True)
        return
    await interaction.response.defer()
    gid = interaction.guild_id
    best = await adb(f"SELECT player_name, SUM(seconds) AS t FROM playtime WHERE guild_id = ? AND {like_clause('player_name')} "
                     "GROUP BY player_name ORDER BY t DESC LIMIT 1", (gid, like(joueur)), fetch="one")
    if not best:
        await interaction.followup.send(f"❌ Aucun temps de jeu enregistré pour **{nm(joueur)}**.")
        return
    player, total_sec = best
    rows = (await adb("SELECT map_name, SUM(seconds) FROM playtime WHERE guild_id = ? AND player_name = ? "
                      "GROUP BY map_name ORDER BY 2 DESC", (gid, player), fetch="all")) or []
    bl_rows = await adb("SELECT player_name FROM blacklist WHERE guild_id = ?", (gid,), fetch="all")
    tr_rows = await adb("SELECT player_name, tribe_name FROM player_tribes WHERE guild_id = ?", (gid,), fetch="all")
    blacklist = {r[0].lower() for r in bl_rows or []}
    tribes = {r[0].lower(): r[1] for r in tr_rows or []}
    is_b = is_blacklisted(player, blacklist)
    embed = discord.Embed(title=f"📊 Fiche joueur : {'🟥 ' if is_b else ''}{nm(player)}{tribe_tag(player, tribes)}",
                          color=discord.Color.dark_red() if is_b else discord.Color.blue())
    embed.add_field(name="⏱️ Temps de jeu total", value=f"**{fmt_duration(total_sec)}**", inline=False)
    embed.add_field(name="🗺️ Répartition par map",
                    value="\n".join(f"• **{nm(m)}** : `{fmt_duration(x)}`" for m, x in rows if x)[:1024] or "—", inline=False)
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="historique", description="Dernière connexion d'un joueur sur chaque map (ou qui est passé sur une map).")
@app_commands.guild_only()
@app_commands.describe(joueur="Pseudo (ou début de pseudo)", serveur="Nom de la map")
@app_commands.autocomplete(joueur=ac_joueur, serveur=ac_serveur)
async def historique_slash(interaction: discord.Interaction, joueur: str = None, serveur: str = None):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    g = await get_guild(gid)
    tz = tz_of(g)
    bl_rows = await adb("SELECT player_name FROM blacklist WHERE guild_id = ?", (gid,), fetch="all")
    tr_rows = await adb("SELECT player_name, tribe_name FROM player_tribes WHERE guild_id = ?", (gid,), fetch="all")
    blacklist = {r[0].lower() for r in bl_rows or []}
    tribes = {r[0].lower(): r[1] for r in tr_rows or []}

    query = "SELECT player_name, map_name, last_seen FROM history WHERE guild_id = ?"
    params = [gid]
    if joueur:
        query += f" AND {like_clause('player_name')}"
        params.append(like(joueur.strip()))
    if serveur:
        query += f" AND {like_clause('map_name')}"
        params.append(like(serveur.strip()))
    query += " ORDER BY last_seen DESC LIMIT 400"
    rows = (await adb(query, tuple(params), fetch="all")) or []
    if not rows:
        await interaction.followup.send("❌ Aucun résultat. Vérifie l'orthographe (un début de pseudo suffit).", ephemeral=True)
        return

    now = datetime.now(tz)
    live = online_now(gid)

    def seen(p, m, ls):
        if (m, p) in live:
            return "🟢", "**en ligne maintenant**"
        dt = parse_ts(ls, tz)
        if not dt:
            return "▫️", str(ls)
        return "▫️", f"**{fmt_ago((now - dt).total_seconds())}** · {dt.strftime('%d/%m à %H:%M')}"

    def who(p):
        return f"{'🟥 ' if is_blacklisted(p, blacklist) else ''}**{nm(p)}**{tribe_tag(p, tribes)}"

    blocks = []
    if joueur or not serveur:
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
                lines.append(f"└ *+{len(maps) - cap} autre(s) map(s)*")
            blocks.append("\n".join(lines))
        count = f"{len(by_player)} joueur(s)"
    else:
        by_map = {}
        for p, m, ls in rows:
            by_map.setdefault(m, []).append((p, ls))
        for m, plist in by_map.items():
            lines = [f"🗺️ **{nm(m)}** — {len(plist)} joueur(s)"]
            for p, ls in plist[:12]:
                dot, txt = seen(p, m, ls)
                lines.append(f"└ {dot} {who(p)} — {txt}")
            if len(plist) > 12:
                lines.append(f"└ *+{len(plist) - 12} autre(s)*")
            blocks.append("\n".join(lines))
        count = f"{len(by_map)} map(s)"

    crit = " • ".join(x for x in (f"joueur « {nm(joueur)} »" if joueur else None,
                                  f"map « {nm(serveur)} »" if serveur else None) if x)
    embed = discord.Embed(title="🔍 Historique des connexions",
                          description=f"*{crit or 'dernières connexions'}* — {count}\n\n{pack_blocks(blocks)}",
                          color=discord.Color.blue())
    embed.set_footer(text=f"Heures en {tz.key} • 🟢 en ligne • ▫️ hors ligne • 🟥 blacklist")
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="stats", description="Statistiques de ce serveur Discord.")
@app_commands.guild_only()
async def stats_slash(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    gid = interaction.guild_id
    row = await adb("SELECT (SELECT COUNT(DISTINCT player_name) FROM history WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM blacklist WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM player_tribes WHERE guild_id = ?), "
                    "(SELECT COUNT(*) FROM servers WHERE guild_id = ?)", (gid, gid, gid, gid), fetch="one")
    u, b, t, s = row if row else (0, 0, 0, 0)
    embed = discord.Embed(title="📊 Statistiques", color=discord.Color.green())
    embed.add_field(name="👥 Joueurs uniques enregistrés", value=str(u), inline=False)
    embed.add_field(name="🖥️ Serveurs de jeu suivis", value=str(s), inline=False)
    embed.add_field(name="🟥 Joueurs blacklistés", value=str(b), inline=False)
    embed.add_field(name="🛡️ Affiliations de tribu", value=str(t), inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="aide", description="Liste des commandes et liens utiles.")
async def aide_slash(interaction: discord.Interaction):
    embed = discord.Embed(title=f"❓ Aide — {BRAND}", color=discord.Color.blue(), description=(
        "**Pour tout le monde**\n"
        "`/historique` — dernière connexion d'un joueur sur chaque map\n"
        "`/infos` — temps de jeu d'un joueur\n"
        "`/stats` — statistiques du serveur Discord\n\n"
        "**Pour les administrateurs** (permission *Gérer le serveur*)\n"
        "`/config salons` • `/config serveur-ajouter` • `/config serveur-retirer` • `/config fuseau` • `/config etat`\n"
        "`/blacklist ajouter|retirer|liste` • `/tribu definir|retirer|liste`\n"
        "`/donnees joueur-supprimer` • `/donnees reinitialiser`"))
    links = []
    if SITE_URL:
        links.append(f"[Site web]({SITE_URL}) • [Confidentialité]({SITE_URL.rstrip('/')}/#confidentialite)")
    if SUPPORT_URL:
        links.append(f"[Support]({SUPPORT_URL})")
    if links:
        embed.add_field(name="Liens", value=" • ".join(links), inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


# ======================================================================================
# COMMANDES DU PROPRIÉTAIRE (en mentionnant le bot : "@TribeSpy premium 123 premium")
# ======================================================================================
@bot.command(name="premium")
@commands.is_owner()
async def owner_premium(ctx, guild_id: int, plan: str = "premium"):
    """@Bot premium <id du serveur Discord> <free|premium>"""
    if plan not in PLANS:
        await ctx.send(f"Offre inconnue. Choix : {', '.join(PLANS)}")
        return
    await ensure_guild(guild_id)
    await adb("UPDATE guilds SET plan = ? WHERE guild_id = ?", (plan, guild_id))
    await ctx.send(f"✅ Serveur {guild_id} → offre **{plan}**.")


@bot.command(name="sync")
@commands.is_owner()
async def owner_sync(ctx):
    synced = await bot.tree.sync()
    await ctx.send(f"✅ {len(synced)} commandes synchronisées.")


@bot.command(name="bilan")
@commands.is_owner()
async def owner_bilan(ctx):
    rows = await adb("SELECT plan, COUNT(*) FROM guilds WHERE left_at IS NULL GROUP BY plan", fetch="all")
    n_srv = await adb("SELECT COUNT(*) FROM servers", fetch="one")
    await ctx.send(f"📈 Serveurs Discord : {len(bot.guilds)} • par offre : {dict(rows or [])} • serveurs de jeu suivis : {n_srv[0]}")


# ======================================================================================
# SERVEUR WEB (santé / keep-alive hébergeur)
# ======================================================================================
async def handle_ping(request):
    return web.Response(text=f"{BRAND} actif")


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(os.environ.get("PORT", 10000))).start()


if __name__ == "__main__":
    if not TOKEN:
        print("CRITICAL: DISCORD_TOKEN manquant !")
    else:
        bot.run(TOKEN, log_handler=None)
