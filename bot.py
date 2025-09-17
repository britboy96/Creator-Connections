"""
Creators Connections — TikTok → Discord Graphic Leaderboard Bot (no webhooks)

UPDATES INCLUDED
1) Health ping auto-checks if host is live; if live and not already tracking, auto-starts (no spam).
2) XP system on gifts (diamonds if present else 100 XP). Rank-ups announced (Bronze → Unreal).
3) Image text auto-resize + ellipsis and true vertical centering.
4) Custom live-recap caption via POST_LIVE_MESSAGE.
5) Weekly summary fixed:
   - Overlap-window query so sessions starting before the week but ending inside it count.
   - Includes current in-memory tallies for any ongoing or unflushed live, so it never posts empty.
6) Monthly XP tally: 1st of each month @ 12:00 — lists EVERYONE grouped by current rank (deduped).

Run:
    python bot.py
"""

from __future__ import annotations

import os
import io
import re
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import aiosqlite
import pytz
from dotenv import load_dotenv

from PIL import Image, ImageDraw, ImageFont

import discord
from discord import app_commands

from TikTokLive import TikTokLiveClient
from TikTokLive.events import GiftEvent, LiveEndEvent, CommentEvent, ConnectEvent, LikeEvent
from aiohttp import web, ClientSession, ClientTimeout

# ------------------- Config -------------------
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DEFAULT_TZ = os.getenv("DEFAULT_TIMEZONE", "Etc/UTC")
DB_PATH = os.getenv("DB_PATH", "supporters.db")
ASSETS_DIR = os.getenv("ASSETS_DIR", "assets")
BACKGROUND_IMAGE = os.getenv("BACKGROUND_IMAGE", os.path.join(ASSETS_DIR, "creators_connections_bg.png"))
PORT = int(os.getenv("PORT", "8080"))
CONNECT_PROMPT_TEXT = os.getenv(
    "CONNECT_PROMPT_TEXT",
    "🔗 Connect your TikTok to your Discord so you can appear on the board and earn roles!\n"
    "Use: `/tokconnect your_tiktok_name` (no @)"
)
POST_LIVE_MESSAGE = os.getenv(
    "POST_LIVE_MESSAGE",
    "🎬 **Creators Connections — Live Recap**\nLeft: Top Gifters • Right: Top Tappers"
)
DEBUG_TIKTOK = os.getenv("DEBUG_TIKTOK", "false").lower() == "true"
TIKTOK_SESSIONID = os.getenv("TIKTOK_SESSIONID", "").strip()

# XP / Ranks
DEFAULT_XP_PER_GIFT = int(os.getenv("DEFAULT_XP_PER_GIFT", "100"))
RANKS = [
    ("Bronze", 0),
    ("Silver", 1500),
    ("Gold", 3500),
    ("Platinum", 6500),
    ("Diamond", 10500),
    ("Elite", 15500),
    ("Champion", 21500),
    ("Unreal", 28500),
]

# ------------------- Utility -------------------
def now_tz(tz_name: str = DEFAULT_TZ) -> datetime:
    return datetime.now(pytz.timezone(tz_name))

async def ensure_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id INTEGER PRIMARY KEY,
                tiktok_username TEXT,
                channel_id INTEGER,
                top_role_id INTEGER,
                timezone TEXT DEFAULT 'Etc/UTC',
                weekly_day INTEGER DEFAULT 6,
                weekly_hour INTEGER DEFAULT 19,
                weekly_minute INTEGER DEFAULT 0
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS link_map (
                guild_id INTEGER,
                tiktok_username TEXT,
                discord_user_id INTEGER,
                PRIMARY KEY (guild_id, tiktok_username)
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS live_session (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                tiktok_username TEXT,
                started_at TEXT,
                ended_at TEXT
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS live_gift (
                session_id INTEGER,
                guild_id INTEGER,
                tiktok_user TEXT,
                count INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS live_comment (
                session_id INTEGER,
                guild_id INTEGER,
                tiktok_user TEXT,
                count INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS live_like (
                session_id INTEGER,
                guild_id INTEGER,
                tiktok_user TEXT,
                count INTEGER
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS user_xp (
                guild_id INTEGER,
                discord_user_id INTEGER,
                xp INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, discord_user_id)
            );
        """)
        # track monthly XP post dedupe
        await db.execute("""
            CREATE TABLE IF NOT EXISTS monthly_posted (
                guild_id INTEGER,
                yyyymm TEXT,
                PRIMARY KEY (guild_id, yyyymm)
            );
        """)
        await db.commit()

async def get_guild_cfg(guild_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT tiktok_username, channel_id, top_role_id, timezone, weekly_day, weekly_hour, weekly_minute
            FROM guild_config WHERE guild_id=?
        """, (guild_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                return {}
            return {
                "tiktok_username": row[0],
                "channel_id": row[1],
                "top_role_id": row[2],
                "timezone": row[3] or DEFAULT_TZ,
                "weekly_day": row[4] or 6,
                "weekly_hour": row[5] or 19,
                "weekly_minute": row[6] or 0,
            }

async def upsert_guild_cfg(guild_id: int, **kwargs):
    cfg = await get_guild_cfg(guild_id)
    cfg.update(kwargs)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO guild_config (guild_id, tiktok_username, channel_id, top_role_id, timezone, weekly_day, weekly_hour, weekly_minute)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
              tiktok_username=excluded.tiktok_username,
              channel_id=excluded.channel_id,
              top_role_id=excluded.top_role_id,
              timezone=excluded.timezone,
              weekly_day=excluded.weekly_day,
              weekly_hour=excluded.weekly_hour,
              weekly_minute=excluded.weekly_minute;
        """, (
            guild_id,
            cfg.get("tiktok_username"),
            cfg.get("channel_id"),
            cfg.get("top_role_id"),
            cfg.get("timezone", DEFAULT_TZ),
            cfg.get("weekly_day", 6),
            cfg.get("weekly_hour", 19),
            cfg.get("weekly_minute", 0),
        ))
        await db.commit()

def yyyymm(dt: datetime) -> str:
    return dt.strftime("%Y%m")

# ------------------- Discord Setup -------------------
intents = discord.Intents.default()
intents.members = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

running_clients: Dict[int, TikTokLiveClient] = {}
current_session_id: Dict[int, int] = {}
live_gifters: Dict[int, Dict[str, int]] = {}
live_commenters: Dict[int, Dict[str, int]] = {}
live_likers: Dict[int, Dict[str, int]] = {}

_last_auto_start: Dict[int, float] = {}  # throttle for auto-starts

# ------------------- Image Generation -------------------
def load_font(size: int) -> ImageFont.FreeTypeFont:
    ttf_candidates = [
        os.path.join(ASSETS_DIR, "Montserrat-Bold.ttf"),
        os.path.join(ASSETS_DIR, "Inter-Bold.ttf"),
        os.path.join(ASSETS_DIR, "Arial.ttf"),
    ]
    for path in ttf_candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size=size)
            except Exception:
                pass
    return ImageFont.load_default()

def _fit_font(draw: ImageDraw.ImageDraw, text: str, max_width: int, font_fn, min_size=20, max_size=64):
    lo, hi = min_size, max_size
    best = font_fn(min_size)
    while lo <= hi:
        mid = (lo + hi) // 2
        f = font_fn(mid)
        l, t, r, b = draw.textbbox((0, 0), text, font=f)
        if (r - l) <= max_width:
            best = f
            lo = mid + 1
        else:
            hi = mid - 1
    return best

def _ellipsis_to_fit(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> str:
    l, t, r, b = draw.textbbox((0, 0), text, font=font)
    if (r - l) <= max_width:
        return text
    prefix = "@"
    core = text[1:] if text.startswith("@") else text
    while core and draw.textlength(prefix + core + "…", font=font) > max_width:
        core = core[:-1]
    return (prefix + core + "…") if core else "…"

def draw_creators_connections_template(
    left_rows: List[Tuple[str, int]],
    right_rows: List[Tuple[str, int]]
) -> bytes:
    if not os.path.exists(BACKGROUND_IMAGE):
        raise FileNotFoundError(f"BACKGROUND_IMAGE not found: {BACKGROUND_IMAGE}")

    bg = Image.open(BACKGROUND_IMAGE).convert("RGBA")
    W, H = bg.size

    canvas = Image.new("RGBA", (W, H))
    canvas.alpha_composite(bg)
    d = ImageDraw.Draw(canvas)
    WHITE = (255, 255, 255, 255)

    ROWS = 10
    TABLE_TOP    = int(0.355 * H)
    TABLE_BOTTOM = int(0.905 * H)
    row_height = (TABLE_BOTTOM - TABLE_TOP) // ROWS

    LEFT_X   = int(0.205 * W)
    RIGHT_X  = int(0.585 * W)
    CELL_W   = int(0.315 * W)

    def centered_draw(name: str, row_index: int, col_x: int):
        font = _fit_font(d, name, CELL_W, load_font, min_size=20, max_size=64)
        txt = _ellipsis_to_fit(d, name, font, CELL_W)
        l, t, r, b = d.textbbox((0, 0), txt, font=font)
        text_w, text_h = (r - l), (b - t)
        row_top = TABLE_TOP + row_index * row_height
        row_center_y = row_top + row_height // 2
        x = col_x + (CELL_W - text_w) // 2
        y = row_center_y - text_h // 2
        d.text((x, y), txt, font=font, fill=WHITE)

    for i in range(ROWS):
        if i < len(left_rows):
            centered_draw(str(left_rows[i][0]), i, LEFT_X)
        if i < len(right_rows):
            centered_draw(str(right_rows[i][0]), i, RIGHT_X)

    out = io.BytesIO()
    canvas.convert("RGB").save(out, format="PNG")
    return out.getvalue()

# ------------------- Role Helpers -------------------
async def ensure_named_role(guild: discord.Guild, name: str) -> Optional[discord.Role]:
    role = discord.utils.get(guild.roles, name=name)
    if role:
        return role
    try:
        return await guild.create_role(name=name, reason=f"Auto-create role {name}")
    except Exception:
        return None

async def rotate_single_holder_role(guild: discord.Guild, role: discord.Role, winner: discord.Member, reason: str):
    for m in guild.members:
        if role in m.roles and m.id != winner.id:
            try:
                await m.remove_roles(role, reason=reason)
            except Exception:
                pass
    if role not in winner.roles:
        try:
            await winner.add_roles(role, reason=reason)
        except Exception:
            pass

# ------------------- XP Helpers -------------------
def _rank_for_xp(xp: int) -> Tuple[str, int]:
    current_name, current_idx = RANKS[0][0], 0
    for idx, (name, thresh) in enumerate(RANKS):
        if xp >= thresh:
            current_name, current_idx = name, idx
        else:
            break
    return current_name, current_idx

async def _award_xp_for_tiktok_user(guild: discord.Guild, tiktok_user: str, xp_gain: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT discord_user_id FROM link_map WHERE guild_id=? AND tiktok_username=?",
            (guild.id, tiktok_user)
        ) as cur:
            row = await cur.fetchone()

    if not row:
        return

    discord_user_id = row[0]

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT xp FROM user_xp WHERE guild_id=? AND discord_user_id=?",
            (guild.id, discord_user_id)
        ) as cur:
            row2 = await cur.fetchone()
        if not row2:
            await db.execute(
                "INSERT INTO user_xp (guild_id, discord_user_id, xp) VALUES (?, ?, ?)",
                (guild.id, discord_user_id, 0)
            )
            await db.commit()
            old_xp = 0
        else:
            old_xp = int(row2[0])

    new_xp = old_xp + xp_gain
    old_rank, old_idx = _rank_for_xp(old_xp)
    new_rank, new_idx = _rank_for_xp(new_xp)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE user_xp SET xp=? WHERE guild_id=? AND discord_user_id=?",
            (new_xp, guild.id, discord_user_id)
        )
        await db.commit()

    if new_idx > old_idx:
        member = guild.get_member(discord_user_id) or await guild.fetch_member(discord_user_id)
        if member:
            cfg = await get_guild_cfg(guild.id)
            ch = guild.get_channel(cfg.get("channel_id")) or guild.system_channel
            if ch:
                await ch.send(f"🏅 {member.mention} ranked up! **{old_rank} → {new_rank}** (XP: {new_xp:,})")

# ------------------- TikTok Handling -------------------
def _user_id_from_event_user(u) -> str:
    for attr in ("uniqueId", "unique_id", "username"):
        if hasattr(u, attr):
            val = getattr(u, attr)
            if isinstance(val, str) and val:
                return val
    if hasattr(u, "nickname") and isinstance(u.nickname, str) and u.nickname:
        return u.nickname
    if hasattr(u, "id"):
        return str(getattr(u, "id"))
    return "unknown_user"

async def start_tiktok(guild: discord.Guild):
    cfg = await get_guild_cfg(guild.id)

    username = cfg.get("tiktok_username")
    if not username:
        raise RuntimeError("❌ No TikTok username set. Use `/toktrack <username>` first.")
    username = str(username).strip().lstrip("@")

    channel_id = cfg.get("channel_id")
    if not channel_id:
        raise RuntimeError("❌ No target channel set. Use `/set_target_channel #channel` first.")

    await stop_tiktok(guild)

    try:
        client = TikTokLiveClient(unique_id=username)
        sess = TIKTOK_SESSIONID
        if sess:
            try:
                http = getattr(client, "http", None) or getattr(client, "_client", None)
                if http and hasattr(http, "cookies"):
                    try:
                        http.cookies.set("sessionid", sess, domain=".tiktok.com")
                    except Exception:
                        http.cookies.set("sessionid", sess)
                elif http and hasattr(http, "cookie_jar"):
                    try:
                        http.cookie_jar.update_cookies({"sessionid": sess}, response_url="https://www.tiktok.com/")
                    except Exception:
                        http.cookie_jar.update_cookies({"sessionid": sess})
                if hasattr(client, "headers") and isinstance(client.headers, dict):
                    base = client.headers.get("cookie", "").strip()
                    add = f"sessionid={sess}"
                    if base:
                        if "sessionid=" not in base:
                            client.headers["cookie"] = base + "; " + add
                    else:
                        client.headers["cookie"] = add
            except Exception:
                if hasattr(client, "headers") and isinstance(client.headers, dict):
                    client.headers["cookie"] = f"sessionid={sess}"
    except Exception as e:
        raise RuntimeError(f"Failed to create TikTok client for @{username}: {e}")

    running_clients[guild.id] = client
    live_gifters[guild.id] = {}
    live_commenters[guild.id] = {}
    live_likers[guild.id] = {}

    async def open_session():
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "INSERT INTO live_session (guild_id, tiktok_username, started_at) VALUES (?, ?, ?)",
                (guild.id, username, now_tz(cfg.get("timezone", DEFAULT_TZ)).isoformat())
            )
            await db.commit()
            return cur.lastrowid

    @client.on(ConnectEvent)
    async def on_connect(_: ConnectEvent):
        sid = await open_session()
        current_session_id[guild.id] = sid
        ch = guild.get_channel(channel_id)
        if ch:
            await ch.send(f"🟢 Tracking started for TikTok **@{username}**.")

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        try:
            user = _user_id_from_event_user(event.user)
            repeat = int(getattr(event.gift, "repeatCount", 1) or 1)
            diamonds = getattr(event.gift, "diamond_count", None) or getattr(event.gift, "diamondCount", None)
            amount = repeat
            live_gifters[guild.id][user] = live_gifters[guild.id].get(user, 0) + amount

            xp_gain = (diamonds if (isinstance(diamonds, int) and diamonds > 0) else DEFAULT_XP_PER_GIFT) * repeat
            asyncio.create_task(_award_xp_for_tiktok_user(guild, user, xp_gain))

            if DEBUG_TIKTOK:
                ch = guild.get_channel(channel_id)
                if ch:
                    await ch.send(f"[debug] gift from @{user} (+{amount}) xp+{xp_gain}")
        except Exception as e:
            ch = guild.get_channel(channel_id)
            if ch and DEBUG_TIKTOK:
                await ch.send(f"[debug] gift handler error: {e}")

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        try:
            user = _user_id_from_event_user(event.user)
            live_commenters[guild.id][user] = live_commenters[guild.id].get(user, 0) + 1
            if DEBUG_TIKTOK:
                ch = guild.get_channel(channel_id)
                if ch:
                    await ch.send(f"[debug] comment by @{user}")
        except Exception as e:
            ch = guild.get_channel(channel_id)
            if ch and DEBUG_TIKTOK:
                await ch.send(f"[debug] comment handler error: {e}")

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):
        try:
            user = _user_id_from_event_user(event.user)
            cnt = int(getattr(event, "likeCount", 1) or 1)
            live_likers[guild.id][user] = live_likers[guild.id].get(user, 0) + cnt
            if DEBUG_TIKTOK:
                ch = guild.get_channel(channel_id)
                if ch:
                    await ch.send(f"[debug] +{cnt} likes by @{user}")
        except Exception as e:
            ch = guild.get_channel(channel_id)
            if ch and DEBUG_TIKTOK:
                await ch.send(f"[debug] like handler error: {e}")

    @client.on(LiveEndEvent)
    async def on_live_end(event: LiveEndEvent):
        cfg_local = await get_guild_cfg(guild.id)
        tz = cfg_local.get("timezone", DEFAULT_TZ)
        channel = guild.get_channel(cfg_local.get("channel_id"))
        sid = current_session_id.get(guild.id)

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE live_session SET ended_at=? WHERE id=?", (now_tz(tz).isoformat(), sid))
            for user, cnt in live_gifters[guild.id].items():
                await db.execute("INSERT INTO live_gift VALUES (?, ?, ?, ?)", (sid, guild.id, user, cnt))
            for user, cnt in live_commenters[guild.id].items():
                await db.execute("INSERT INTO live_comment VALUES (?, ?, ?, ?)", (sid, guild.id, user, cnt))
            for user, cnt in live_likers[guild.id].items():
                await db.execute("INSERT INTO live_like VALUES (?, ?, ?, ?)", (sid, guild.id, user, cnt))
            await db.commit()

        gifts_sorted = sorted(live_gifters[guild.id].items(), key=lambda x: x[1], reverse=True)
        tappers_sorted = sorted(live_likers[guild.id].items(), key=lambda x: x[1], reverse=True)

        async def resolve_names(pairs):
            out = []
            async with aiosqlite.connect(DB_PATH) as db:
                for user, score in pairs:
                    async with db.execute(
                        "SELECT discord_user_id FROM link_map WHERE guild_id=? AND tiktok_username=?",
                        (guild.id, user)
                    ) as cur:
                        row = await cur.fetchone()
                    display = f"@{user}"
                    if row:
                        member = guild.get_member(row[0]) or await guild.fetch_member(row[0])
                        if member:
                            display = member.display_name
                    out.append((display, score))
            return out

        gifts_display = await resolve_names(gifts_sorted)
        taps_display = await resolve_names(tappers_sorted)

        if channel:
            cc_img = draw_creators_connections_template(gifts_display, taps_display)
            await channel.send(
                POST_LIVE_MESSAGE,
                file=discord.File(io.BytesIO(cc_img), filename="creators_connections.png")
            )

        if gifts_sorted:
            top_tiktok = gifts_sorted[0][0]
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT discord_user_id FROM link_map WHERE guild_id=? AND tiktok_username=?",
                    (guild.id, top_tiktok)
                ) as cur:
                    row = await cur.fetchone()
            if row:
                member = guild.get_member(row[0]) or await guild.fetch_member(row[0])
                if member:
                    top_role = await ensure_named_role(guild, "Top Gifter")
                    if top_role:
                        await rotate_single_holder_role(guild, top_role, member, "Top gifter of last live")

        live_gifters[guild.id].clear()
        live_commenters[guild.id].clear()
        live_likers[guild.id].clear()

    asyncio.create_task(client.start())

async def stop_tiktok(guild: discord.Guild):
    client = running_clients.get(guild.id)
    if client:
        try:
            await client.stop()
        except Exception:
            pass
        running_clients.pop(guild.id, None)

# ------------------- Weekly Summary (fixed) + Sore Finger -------------------
async def compute_weekly_lists(guild_id: int, start: datetime, end: datetime):
    """
    Returns (gifts_sorted, likes_sorted) for sessions that OVERLAP [start, end].
    Also merges in-memory tallies for any currently running session so we never post blank.
    """
    gifts: Dict[str, int] = {}
    likes: Dict[str, int] = {}

    # DB totals for overlapping sessions
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id FROM live_session
            WHERE guild_id=?
              AND started_at <= ?
              AND (ended_at IS NULL OR ended_at >= ?)
            """,
            (guild_id, end.isoformat(), start.isoformat())
        ) as cur:
            sids = [row[0] for row in await cur.fetchall()]

        if sids:
            qmarks = ",".join(["?"] * len(sids))
            async with db.execute(
                f"SELECT tiktok_user, SUM(count) FROM live_gift WHERE session_id IN ({qmarks}) GROUP BY tiktok_user",
                sids
            ) as cur:
                for u, total in await cur.fetchall():
                    gifts[u] = gifts.get(u, 0) + int(total or 0)
            async with db.execute(
                f"SELECT tiktok_user, SUM(count) FROM live_like WHERE session_id IN ({qmarks}) GROUP BY tiktok_user",
                sids
            ) as cur:
                for u, total in await cur.fetchall():
                    likes[u] = likes.get(u, 0) + int(total or 0)

    # Merge current in-memory tallies (covers ongoing live or missed flush)
    gifts_live = live_gifters.get(guild_id, {})
    likes_live = live_likers.get(guild_id, {})
    for u, c in gifts_live.items():
        gifts[u] = gifts.get(u, 0) + int(c)
    for u, c in likes_live.items():
        likes[u] = likes.get(u, 0) + int(c)

    gifts_sorted = sorted(gifts.items(), key=lambda x: x[1], reverse=True)
    likes_sorted = sorted(likes.items(), key=lambda x: x[1], reverse=True)
    return gifts_sorted, likes_sorted

async def post_weekly_summary(guild_id: int):
    guild = bot.get_guild(guild_id)
    if guild is None:
        return
    cfg = await get_guild_cfg(guild_id)
    ch = guild.get_channel(cfg.get("channel_id"))
    if ch is None:
        return
    tz = pytz.timezone(cfg.get("timezone", DEFAULT_TZ))
    end = datetime.now(tz)
    start = end - timedelta(days=7)

    gifts, likes = await compute_weekly_lists(guild_id, start, end)

    # name resolution
    async def resolve_names(pairs):
        out = []
        async with aiosqlite.connect(DB_PATH) as db:
            for user, score in pairs:
                async with db.execute(
                    "SELECT discord_user_id FROM link_map WHERE guild_id=? AND tiktok_username=?",
                    (guild.id, user)
                ) as cur:
                    row = await cur.fetchone()
                display = f"@{user}"
                if row:
                    member = guild.get_member(row[0]) or await guild.fetch_member(row[0])
                    if member:
                        display = member.display_name
                out.append((display, score))
        return out

    gifts_display = await resolve_names(gifts)
    taps_display = await resolve_names(likes)

    img = draw_creators_connections_template(gifts_display, taps_display)
    await ch.send(
        "📅 **Creators Connections — Weekly Summary**\nLeft: Top Gifters • Right: Top Tappers",
        file=discord.File(io.BytesIO(img), filename="creators_connections_weekly.png")
    )
    await ch.send("🔗 Link your TikTok with `/tokconnect your_tiktok_name` (without @) to get ranked!")

    if likes:
        top_tiktok = likes[0][0]
        role = await ensure_named_role(guild, "Sore Finger")
        if role:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT discord_user_id FROM link_map WHERE guild_id=? AND tiktok_username=?",
                    (guild.id, top_tiktok)
                ) as cur:
                    row = await cur.fetchone()
            if row:
                winner = guild.get_member(row[0]) or await guild.fetch_member(row[0])
                if winner:
                    await rotate_single_holder_role(guild, role, winner, "Weekly top tapper")
                    sysch = guild.system_channel or ch
                    await sysch.send(f"🖐️ {winner.mention} now has sore fingers!")

# ------------------- Monthly XP Tally -------------------
async def post_monthly_xp_tally(guild_id: int):
    guild = bot.get_guild(guild_id)
    if guild is None:
        return
    cfg = await get_guild_cfg(guild_id)
    ch = guild.get_channel(cfg.get("channel_id")) or guild.system_channel
    if ch is None:
        return

    # dedupe check
    tz = pytz.timezone(cfg.get("timezone", DEFAULT_TZ))
    stamp = yyyymm(datetime.now(tz))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM monthly_posted WHERE guild_id=? AND yyyymm=?",
            (guild_id, stamp)
        ) as cur:
            if await cur.fetchone():
                return  # already posted this month
        await db.execute("INSERT OR IGNORE INTO monthly_posted (guild_id, yyyymm) VALUES (?, ?)", (guild_id, stamp))
        await db.commit()

    # Load all XP and group by rank
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT discord_user_id, xp FROM user_xp WHERE guild_id=? ORDER BY xp DESC",
            (guild_id,)
        ) as cur:
            rows = await cur.fetchall()

    if not rows:
        await ch.send("📊 **Monthly XP Tally** — No data yet.")
        return

    groups: Dict[str, List[Tuple[int, int]]] = {}
    for uid, xp in rows:
        rank, _ = _rank_for_xp(int(xp or 0))
        groups.setdefault(rank, []).append((int(uid), int(xp)))

    # order ranks by RANKS array
    rank_order = [name for name, _ in RANKS]
    header = "📊 **Monthly XP Tally** — Everyone by current rank"
    await ch.send(header)

    for rank_name in rank_order:
        members = groups.get(rank_name, [])
        if not members:
            continue
        # build chunked messages to respect 2000-char limit
        lines = [f"**{rank_name}**"]
        current_block = ""
        for uid, xp in members:
            member = guild.get_member(uid) or (await guild.fetch_member(uid) if guild.get_member(uid) is None else None)
            label = member.display_name if member else f"<@{uid}>"
            entry = f"- {label} — {xp:,} XP\n"
            if len(current_block) + len(entry) > 1800:  # start a new message
                await ch.send("\n".join(lines) + "\n" + f"```{current_block}```")
                lines = [f"**{rank_name}** (cont.)"]
                current_block = entry
            else:
                current_block += entry
        if current_block:
            await ch.send("\n".join(lines) + "\n" + f"```{current_block}```")

# ------------------- Health / Uptime auto-start -------------------
async def _is_tiktok_live(username: str) -> bool:
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    }
    cookies = {}
    if TIKTOK_SESSIONID:
        cookies["sessionid"] = TIKTOK_SESSIONID

    timeout = ClientTimeout(total=6)
    async with ClientSession(headers=headers, cookies=cookies, timeout=timeout) as session:
        try:
            url = f"https://www.tiktok.com/api/live/detail/?aid=1988&uniqueId={username}"
            async with session.get(url) as r:
                txt = await r.text()
                low = txt.lower()
                if '"islive":true' in low or '"status":1' in low or '"live_room_id"' in low:
                    return True
        except Exception:
            pass
        try:
            url2 = f"https://www.tiktok.com/@{username}"
            async with session.get(url2) as r2:
                page = (await r2.text()).lower()
                if '"islive":true' in page or '"roomid":"' in page or '"live_room_id"' in page:
                    return True
        except Exception:
            pass
    return False

async def _health_tick():
    for guild in list(bot.guilds):
        try:
            cfg = await get_guild_cfg(guild.id)
            username = (cfg.get("tiktok_username") or "").strip().lstrip("@")
            if not username:
                continue
            if running_clients.get(guild.id):
                continue
            prev = _last_auto_start.get(guild.id, 0.0)
            if time.time() - prev < 90:
                continue
            if await _is_tiktok_live(username):
                _last_auto_start[guild.id] = time.time()
                await start_tiktok(guild)
        except Exception:
            pass

async def _ok(_: web.Request) -> web.Response:
    asyncio.create_task(_health_tick())
    return web.Response(text="ok")

async def start_keepalive():
    app = web.Application()
    app.add_routes([web.get("/", _ok), web.get("/health", _ok)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    print(f"Keep-alive server running on 0.0.0.0:{PORT}")

# ------------------- Schedulers -------------------
async def scheduler():
    await bot.wait_until_ready()
    while not bot.is_closed():
        for guild in bot.guilds:
            cfg = await get_guild_cfg(guild.id)
            tz = pytz.timezone(cfg.get("timezone", DEFAULT_TZ))
            now = datetime.now(tz)

            # Weekly summary
            if (now.isoweekday() == (cfg.get("weekly_day") or 6)
                and now.hour == (cfg.get("weekly_hour") or 19)
                and now.minute == (cfg.get("weekly_minute") or 0)):
                await post_weekly_summary(guild.id)

            # Monthly XP tally — 1st @ 12:00
            if (now.day == 1 and now.hour == 12 and now.minute == 0):
                await post_monthly_xp_tally(guild.id)

        await asyncio.sleep(60)

# ------------------- Commands -------------------
@tree.command(name="tokconnect", description="Link your TikTok username to your Discord (viewer-level)")
async def tokconnect(interaction: discord.Interaction, username: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    handle = username.strip().lstrip("@")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO link_map VALUES (?, ?, ?) "
            "ON CONFLICT(guild_id, tiktok_username) DO UPDATE SET discord_user_id=excluded.discord_user_id",
            (interaction.guild_id, handle, interaction.user.id)
        )
        await db.commit()
    await interaction.followup.send(f"🔗 Linked @{handle} → {interaction.user.mention}", ephemeral=True)

@tree.command(name="toktrack", description="Admin: set the TikTok host account to track")
@app_commands.checks.has_permissions(manage_guild=True)
async def toktrack(interaction: discord.Interaction, username: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await upsert_guild_cfg(interaction.guild_id, tiktok_username=username.strip().lstrip('@'))
    await interaction.followup.send(f"✅ Host set to @{username.strip().lstrip('@')}", ephemeral=True)

@tree.command(name="set_target_channel", description="Set the channel for leaderboard posts")
async def set_target_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await upsert_guild_cfg(interaction.guild_id, channel_id=channel.id)
    await interaction.followup.send(f"Target channel set to {channel.mention}", ephemeral=True)

@tree.command(name="start_tiktok", description="Start TikTok tracking for this server")
async def start_cmd(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True, thinking=True)
        await start_tiktok(interaction.guild)
        await interaction.followup.send("🟢 Started TikTok tracking.", ephemeral=True)
    except Exception as e:
        if interaction.response.is_done():
            await interaction.followup.send(f"⚠️ {e}", ephemeral=True)
        else:
            await interaction.response.send_message(f"⚠️ {e}", ephemeral=True)

@tree.command(name="stop_tiktok", description="Stop TikTok tracking")
async def stop_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    await stop_tiktok(interaction.guild)
    await interaction.followup.send("🛑 Stopped TikTok tracking.", ephemeral=True)

@tree.command(name="post_connect_prompt", description="Post & pin the connect prompt (admin)")
@app_commands.checks.has_permissions(manage_guild=True)
async def post_connect_prompt_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    cfg = await get_guild_cfg(interaction.guild_id)
    ch_id = cfg.get("channel_id")
    channel = interaction.guild.get_channel(ch_id) if ch_id else None
    if not channel:
        await interaction.followup.send("❌ Set a target channel first with /set_target_channel", ephemeral=True)
        return
    msg = await channel.send(CONNECT_PROMPT_TEXT)
    try:
        await msg.pin()
    except Exception:
        pass
    await interaction.followup.send("✅ Posted and pinned connect prompt.", ephemeral=True)

@tree.command(name="backscan", description="Admin: scan recent messages for TikTok handles/links and auto-link authors")
@app_commands.describe(limit="Messages to scan (10–2000)", channel="Channel to scan (defaults to target channel)")
@app_commands.checks.has_permissions(manage_guild=True)
async def backscan(interaction: discord.Interaction, limit: app_commands.Range[int, 10, 2000]=200, channel: Optional[discord.TextChannel]=None):
    await interaction.response.defer(ephemeral=True, thinking=True)
    cfg = await get_guild_cfg(interaction.guild_id)
    scan_ch = channel or interaction.guild.get_channel(cfg.get("channel_id"))
    if not scan_ch:
        await interaction.followup.send("❌ No channel to scan. Set one via /set_target_channel or pass a channel.", ephemeral=True)
        return
    pattern = re.compile(r"(?:tiktok\.com/\@|\B\@)([A-Za-z0-9._-]{2,24})")
    found: Dict[int, set[str]] = {}
    async for msg in scan_ch.history(limit=limit):
        for m in pattern.finditer(msg.content or ""):
            handle = m.group(1).strip("@")
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO link_map (guild_id, tiktok_username, discord_user_id) VALUES (?, ?, ?) "
                    "ON CONFLICT(guild_id, tiktok_username) DO UPDATE SET discord_user_id=excluded.discord_user_id",
                    (interaction.guild_id, handle, msg.author.id)
                )
                await db.commit()
            found.setdefault(msg.author.id, set()).add(handle)
    if not found:
        await interaction.followup.send("No TikTok handles found in recent messages.", ephemeral=True)
        return
    lines = ["**Backscan results:**"]
    for uid, handles in found.items():
        member = interaction.guild.get_member(uid) or await interaction.guild.fetch_member(uid)
        lines.append(f"• {member.display_name}: " + ", ".join(f"@{h}" for h in sorted(handles)))
    await interaction.followup.send("\n".join(lines), ephemeral=True)

@tree.command(name="cc_test_image", description="(Admin) Post a test leaderboard with dummy data")
@app_commands.checks.has_permissions(manage_guild=True)
async def cc_test_image(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False, thinking=True)
    left = [(f"userGifter{i}", 110 - i * 10) for i in range(1, 11)]
    right = [(f"userTapper{i}", 5000 - i * 250) for i in range(1, 11)]
    img_bytes = draw_creators_connections_template(left, right)
    await interaction.followup.send(
        "🧪 **Creators Connections — Test Image**\nLeft: Top Gifters • Right: Top Tappers",
        file=discord.File(io.BytesIO(img_bytes), filename="creators_connections_TEST.png"),
    )

@tree.command(name="cc_status", description="Show TikTok tracking status & current tallies")
async def cc_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    client = running_clients.get(interaction.guild_id)
    ggifters = live_gifters.get(interaction.guild_id, {})
    glikers = live_likers.get(interaction.guild_id, {})
    state = "running" if client else "stopped"
    top_gifters = ", ".join([f"@{u}:{c}" for u, c in sorted(ggifters.items(), key=lambda x: x[1], reverse=True)[:5]]) or "none"
    top_likers = ", ".join([f"@{u}:{c}" for u, c in sorted(glikers.items(), key=lambda x: x[1], reverse=True)[:5]]) or "none"
    await interaction.followup.send(
        f"Status: **{state}**\nTop gifters (live): {top_gifters}\nTop likers (live): {top_likers}",
        ephemeral=True
    )

# ------------------- Keep-Alive Web Server -------------------
async def start_keepalive():
    app = web.Application()
    app.add_routes([web.get("/", lambda r: _ok(r)), web.get("/health", lambda r: _ok(r))])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    print(f"Keep-alive server running on 0.0.0.0:{PORT}")

# ------------------- Lifecycle -------------------
@bot.event
async def on_member_join(member: discord.Member):
    try:
        await member.send(
            "👋 Welcome!\n\nTo appear on the Creators Connections board and earn roles like **Top Gifter** or **Sore Finger**, "
            "please link your TikTok by using the command: `/tokconnect your_tiktok_name` (without @)."
        )
    except Exception:
        pass

@bot.event
async def on_guild_join(guild: discord.Guild):
    await ensure_named_role(guild, "Sore Finger")
    await ensure_named_role(guild, "Top Gifter")

@bot.event
async def on_guild_available(guild: discord.Guild):
    await ensure_named_role(guild, "Sore Finger")
    await ensure_named_role(guild, "Top Gifter")

@bot.event
async def on_ready():
    await ensure_db()
    for g in bot.guilds:
        await ensure_named_role(g, "Sore Finger")
        await ensure_named_role(g, "Top Gifter")
    await tree.sync()
    asyncio.create_task(scheduler())
    asyncio.create_task(start_keepalive())
    print(f"Logged in as {bot.user}")

if __name__ == "__main__":
    if not BOT_TOKEN:
        raise SystemExit("❌ Missing DISCORD_BOT_TOKEN in environment")
    print("DISCORD_BOT_TOKEN loaded?", bool(BOT_TOKEN))
    bot.run(BOT_TOKEN)

