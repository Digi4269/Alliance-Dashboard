import os
import time
import asyncio
import threading
import json
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import requests
from flask import Flask, render_template, jsonify

import discord
from discord.ext import commands, tasks
from discord import app_commands

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GGE_BASE = "https://api.gge-tracker.com/api/v1"
GGE_HEADERS = {"gge-server": "E4K_WORLD2"}
ALLIANCE_ID = 4177

EMPIRE_BASE = "https://empire-api.fly.dev"
EMPIRE_SERVER = "EmpirefourkingdomsExGG_37"
EMPIRE_AID = 4

EVENTS = [
    (2, "Plunder Points"),
    (5, "Honor"),
    (6, "Might"),
    (30, "Berimond"),
    (44, "War of the Realms"),
    (46, "Nomad Invasion"),
    (51, "Samurai Invasion"),
    (53, "Season Points"),
    (58, "War of the Bloodcrows"),
]

CACHE_TTL = 300  # 5 minutes

# Discord configuration
DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN", "")
ALLIANCE_ROLE_NAME = "Member"
GUILD_ID = None  # auto-detect from first guild

# ---------------------------------------------------------------------------
# Simple in-memory cache
# ---------------------------------------------------------------------------
_cache: dict = {}


def cache_get(key: str):
    entry = _cache.get(key)
    if entry and (time.time() - entry["ts"] < CACHE_TTL):
        return entry["data"]
    return None


def cache_set(key: str, data):
    _cache[key] = {"data": data, "ts": time.time()}


# ---------------------------------------------------------------------------
# Discord links file (thread-safe)
# ---------------------------------------------------------------------------
LINKS_FILE = Path(__file__).parent / "discord_links.json"
_links_lock = threading.Lock()


def load_links() -> dict:
    """Load discord_links.json. Returns {ingame_name_lower: [discord_user_id, ...]}."""
    with _links_lock:
        try:
            with open(LINKS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


def save_links(links: dict):
    """Save discord_links.json."""
    with _links_lock:
        with open(LINKS_FILE, "w", encoding="utf-8") as f:
            json.dump(links, f, indent=2)


# ---------------------------------------------------------------------------
# Discord activity log (thread-safe, in-memory, max 200 entries)
# ---------------------------------------------------------------------------
_discord_log: list = []
_discord_log_lock = threading.Lock()


def discord_log_add(action: str, discord_user: str, ingame_name: str, detail: str):
    entry = {
        "action": action,
        "discord_user": discord_user,
        "ingame_name": ingame_name,
        "detail": detail,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with _discord_log_lock:
        _discord_log.insert(0, entry)
        if len(_discord_log) > 200:
            _discord_log.pop()


def discord_log_get() -> list:
    with _discord_log_lock:
        return list(_discord_log)


# ---------------------------------------------------------------------------
# Shared Discord member data (thread-safe, read by Flask)
# ---------------------------------------------------------------------------
_discord_members_data: list = []
_discord_data_lock = threading.Lock()


def set_discord_members_data(data: list):
    with _discord_data_lock:
        global _discord_members_data
        _discord_members_data = data


def get_discord_members_data() -> list:
    with _discord_data_lock:
        return list(_discord_members_data)


# ---------------------------------------------------------------------------
# GGE Tracker helpers (sync - requests)
# ---------------------------------------------------------------------------
def _gge_get(path: str):
    url = f"{GGE_BASE}{path}"
    r = requests.get(url, headers=GGE_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_members():
    cached = cache_get("members")
    if cached is not None:
        return cached
    try:
        data = _gge_get(f"/alliances/id/{ALLIANCE_ID}")
        members = data if isinstance(data, list) else data.get("players", data.get("members", data.get("data", [])))
        # Determine birded status from peace_disabled_at
        now = datetime.now(timezone.utc)
        for m in members:
            pda = m.get("peace_disabled_at")
            if pda:
                try:
                    expire = datetime.fromisoformat(pda.replace("Z", "+00:00"))
                    m["birded"] = 1 if expire > now else 0
                except Exception:
                    m["birded"] = 0
            else:
                m["birded"] = 0
        cache_set("members", members)
        return members
    except Exception as e:
        print(f"[ERROR] fetch_members: {e}")
        return []


def fetch_activity():
    cached = cache_get("activity")
    if cached is not None:
        return cached
    try:
        data = _gge_get(f"/updates/alliances/{ALLIANCE_ID}/players")
        items = data if isinstance(data, list) else data.get("data", data.get("updates", []))
        cache_set("activity", items)
        return items
    except Exception as e:
        print(f"[ERROR] fetch_activity: {e}")
        return []


def fetch_renames():
    cached = cache_get("renames")
    if cached is not None:
        return cached
    try:
        data = _gge_get("/server/renames")
        all_renames = data if isinstance(data, list) else data.get("data", data.get("renames", []))
        # Filter: only show renames for players in our alliance
        members = fetch_members()
        member_names = {m.get("player_name", "").lower() for m in members}
        renames = [
            r for r in all_renames
            if r.get("old_player_name", "").lower() in member_names
            or r.get("new_player_name", "").lower() in member_names
            or r.get("alliance_name", "").lower() == "banner of death"
        ]
        cache_set("renames", renames)
        return renames
    except Exception as e:
        print(f"[ERROR] fetch_renames: {e}")
        return []


def fetch_alliance_stats():
    cached = cache_get("alliance_stats")
    if cached is not None:
        return cached
    try:
        data = _gge_get(f"/statistics/alliance/{ALLIANCE_ID}")
        cache_set("alliance_stats", data)
        return data
    except Exception as e:
        print(f"[ERROR] fetch_alliance_stats: {e}")
        return {}


# ---------------------------------------------------------------------------
# Empire Event Scores helpers (async - aiohttp)
# ---------------------------------------------------------------------------
_event_scores_cache: dict = {"data": None, "ts": 0, "loading": False}


async def _fetch_bracket(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                         lt: int, lid: int, alliance_oids: set):
    """Fetch all pages for one bracket. Returns list of (name, score)."""
    results = []
    seen_oids: set = set()
    sv = 2

    while sv < 1202:
        url = (
            f"{EMPIRE_BASE}/{EMPIRE_SERVER}/hgh/"
            f"%22LT%22:{lt},%22LID%22:{lid},%22SV%22:%22{sv}%22"
        )
        async with sem:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        break
                    data = await resp.json(content_type=None)
            except Exception:
                break

        if data.get("return_code") != 0:
            break
        entries = data.get("content", {}).get("L", [])
        if not entries:
            break

        new_count = 0
        for entry in entries:
            player_data = entry[2]
            oid = player_data.get("OID", 0)
            if oid in seen_oids:
                continue
            seen_oids.add(oid)
            new_count += 1
            if player_data.get("AID") == EMPIRE_AID:
                results.append({"name": player_data["N"], "score": entry[1]})

        if new_count == 0:
            break
        sv += 6

    return results


async def _fetch_all_event_scores():
    """Fetch all event brackets concurrently."""
    # First get alliance member names from Empire API
    member_names = set()
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{EMPIRE_BASE}/{EMPIRE_SERVER}/ain/%22AID%22:{EMPIRE_AID}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    for m in data.get("content", {}).get("A", {}).get("M", []):
                        member_names.add(m["N"])
    except Exception as e:
        print(f"[ERROR] fetch empire members: {e}")

    # Fetch all brackets concurrently
    scores: dict = {}  # {name_lower: {event_name: score}}
    sem = asyncio.Semaphore(10)

    async with aiohttp.ClientSession() as session:
        fetch_tasks = []
        task_events = []
        for lt, event_name in EVENTS:
            for lid in range(1, 9):
                fetch_tasks.append(_fetch_bracket(session, sem, lt, lid, set()))
                task_events.append(event_name)

        results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        for event_name, result in zip(task_events, results):
            if isinstance(result, Exception):
                continue
            for entry in result:
                key = entry["name"].lower()
                if key not in scores:
                    scores[key] = {"_name": entry["name"]}
                if scores[key].get(event_name, 0) < entry["score"]:
                    scores[key][event_name] = entry["score"]

    # Build final list - only alliance members
    rows = []
    for name in sorted(member_names, key=str.lower):
        key = name.lower()
        row = {"name": name}
        player_scores = scores.get(key, {})
        for _, event_name in EVENTS:
            row[event_name] = player_scores.get(event_name, 0)
        rows.append(row)

    return rows


def _bg_fetch_event_scores():
    """Run async event score fetching in a background thread."""
    _event_scores_cache["loading"] = True
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        data = loop.run_until_complete(_fetch_all_event_scores())
        loop.close()
        _event_scores_cache["data"] = data
        _event_scores_cache["ts"] = time.time()
    except Exception as e:
        print(f"[ERROR] bg event scores: {e}")
    finally:
        _event_scores_cache["loading"] = False


def get_event_scores():
    """Return cached event scores; trigger background fetch if stale."""
    now = time.time()
    if _event_scores_cache["data"] is not None and (now - _event_scores_cache["ts"] < CACHE_TTL):
        return _event_scores_cache["data"], False

    if not _event_scores_cache["loading"]:
        t = threading.Thread(target=_bg_fetch_event_scores, daemon=True)
        t.start()

    if _event_scores_cache["data"] is not None:
        return _event_scores_cache["data"], False

    return None, True  # still loading


# ---------------------------------------------------------------------------
# Number formatting helper (server-side, also done client-side)
# ---------------------------------------------------------------------------
def fmt_num(n):
    if n is None:
        return "-"
    try:
        n = int(n)
    except (ValueError, TypeError):
        return str(n)
    s = str(abs(n))
    groups = []
    while s:
        groups.append(s[-3:])
        s = s[:-3]
    result = ".".join(reversed(groups))
    return f"-{result}" if n < 0 else result


app.jinja_env.filters["fmt"] = fmt_num


# ---------------------------------------------------------------------------
# Discord Bot
# ---------------------------------------------------------------------------
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Track bot ready state
_bot_ready = False
_bot_guild: discord.Guild = None
_last_sync_time: str = "Never"
_sync_running = False


async def _fetch_empire_members_async() -> list:
    """Fetch current alliance members from Empire API (async)."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{EMPIRE_BASE}/{EMPIRE_SERVER}/ain/%22AID%22:{EMPIRE_AID}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data.get("content", {}).get("A", {}).get("M", [])
    except Exception as e:
        print(f"[DISCORD] Error fetching empire members: {e}")
    return []


async def _fetch_tracker_activity_async() -> list:
    """Fetch GGE Tracker activity log (async)."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{GGE_BASE}/updates/alliances/{ALLIANCE_ID}/players"
            async with session.get(url, headers=GGE_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, list):
                        return data
                    return data.get("updates", data.get("data", []))
    except Exception as e:
        print(f"[DISCORD] Error fetching tracker activity: {e}")
    return []


async def _fetch_tracker_renames_async() -> list:
    """Fetch GGE Tracker renames (async)."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{GGE_BASE}/server/renames"
            async with session.get(url, headers=GGE_HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, list):
                        return data
                    return data.get("renames", data.get("data", []))
    except Exception as e:
        print(f"[DISCORD] Error fetching renames: {e}")
    return []


def _get_effective_name(member: discord.Member) -> str:
    """Get the effective display name (nickname > display_name > name)."""
    return member.nick or member.display_name or member.name


async def do_sync(guild: discord.Guild):
    """Main sync logic: check roles, nicknames, links."""
    global _last_sync_time, _sync_running

    if _sync_running:
        return
    _sync_running = True

    try:
        print("[DISCORD] Starting sync...")

        # 1. Fetch alliance members from Empire API
        empire_members = await _fetch_empire_members_async()
        alliance_names = {m["N"].lower(): m["N"] for m in empire_members}  # lower -> original

        # 2. Fetch tracker activity
        tracker_activity = await _fetch_tracker_activity_async()

        # 3. Fetch renames
        tracker_renames = await _fetch_tracker_renames_async()

        # Build rename lookup: old_name_lower -> new_name
        rename_map = {}
        for r in tracker_renames:
            old = r.get("old_player_name", "")
            new = r.get("new_player_name", "")
            if old and new:
                rename_map[old.lower()] = new

        # Build left-alliance set from activity
        left_alliance = set()
        for item in tracker_activity:
            old_aid = str(item.get("old_alliance_id", ""))
            if old_aid.endswith("177"):
                pname = item.get("player_name", "")
                if pname:
                    left_alliance.add(pname.lower())

        # Find the Member role
        member_role = discord.utils.get(guild.roles, name=ALLIANCE_ROLE_NAME)
        if not member_role:
            print(f"[DISCORD] Role '{ALLIANCE_ROLE_NAME}' not found in guild!")
            discord_log_add("warning", "Bot", "", f"Role '{ALLIANCE_ROLE_NAME}' not found in guild")
            return

        links = load_links()
        links_changed = False

        # Build discord members data for dashboard
        discord_data = []

        # Process all guild members
        for dm in guild.members:
            if dm.bot:
                continue

            has_role = member_role in dm.roles
            effective_name = _get_effective_name(dm)
            effective_name_lower = effective_name.lower() if effective_name else ""
            discord_username = str(dm)

            in_alliance = effective_name_lower in alliance_names
            birded = False  # We don't easily know birded status from Empire API member list

            # Check birded from GGE tracker data (we'd need to cross-reference)
            # For now leave as False

            member_info = {
                "discord_name": discord_username,
                "discord_id": dm.id,
                "ingame_name": "",
                "nickname": dm.nick or "",
                "has_role": has_role,
                "in_alliance": False,
                "birded": False,
                "status": "unknown",
            }

            if has_role:
                # Member HAS the role - check if they should keep it
                if in_alliance:
                    # They're in the alliance - all good
                    member_info["ingame_name"] = alliance_names.get(effective_name_lower, effective_name)
                    member_info["in_alliance"] = True
                    member_info["status"] = "in_alliance"

                    # Update link
                    key = effective_name_lower
                    if key not in links:
                        links[key] = []
                    if dm.id not in links[key]:
                        links[key].append(dm.id)
                        links_changed = True

                    # Auto-fix nickname if needed
                    correct_name = alliance_names.get(effective_name_lower, "")
                    if correct_name and dm.nick != correct_name:
                        try:
                            await dm.edit(nick=correct_name)
                            discord_log_add("nick_changed", discord_username, correct_name,
                                            f"Fixed nickname from '{dm.nick}' to '{correct_name}'")
                            member_info["nickname"] = correct_name
                        except discord.Forbidden:
                            pass
                else:
                    # Name doesn't match alliance - investigate
                    # Check if they left
                    if effective_name_lower in left_alliance:
                        # They left the alliance - remove role
                        try:
                            await dm.remove_roles(member_role)
                            discord_log_add("role_removed", discord_username, effective_name,
                                            "Left the alliance (confirmed by tracker)")
                            member_info["status"] = "left"
                            member_info["has_role"] = False
                        except discord.Forbidden:
                            discord_log_add("warning", discord_username, effective_name,
                                            "Could not remove role (Forbidden)")
                    else:
                        # Check renames
                        new_name = rename_map.get(effective_name_lower)
                        if new_name and new_name.lower() in alliance_names:
                            # They renamed - update nickname and link
                            try:
                                await dm.edit(nick=new_name)
                                discord_log_add("nick_changed", discord_username, new_name,
                                                f"Renamed from '{effective_name}' to '{new_name}'")
                                # Update links
                                old_key = effective_name_lower
                                new_key = new_name.lower()
                                if old_key in links:
                                    old_ids = links.pop(old_key)
                                    if new_key not in links:
                                        links[new_key] = []
                                    for uid in old_ids:
                                        if uid not in links[new_key]:
                                            links[new_key].append(uid)
                                    links_changed = True
                                member_info["ingame_name"] = new_name
                                member_info["nickname"] = new_name
                                member_info["in_alliance"] = True
                                member_info["status"] = "in_alliance"
                            except discord.Forbidden:
                                discord_log_add("warning", discord_username, effective_name,
                                                f"Could not update nickname to '{new_name}' (Forbidden)")
                        else:
                            # Check if any linked name is still in alliance
                            found_link = False
                            for link_name, link_ids in links.items():
                                if dm.id in link_ids and link_name in alliance_names:
                                    # They have a link to an active member name
                                    correct = alliance_names[link_name]
                                    try:
                                        await dm.edit(nick=correct)
                                        discord_log_add("nick_changed", discord_username, correct,
                                                        f"Restored nickname to linked name '{correct}'")
                                        member_info["ingame_name"] = correct
                                        member_info["nickname"] = correct
                                        member_info["in_alliance"] = True
                                        member_info["status"] = "in_alliance"
                                        found_link = True
                                    except discord.Forbidden:
                                        pass
                                    break

                            if not found_link:
                                # No match, no rename, no link - warn but don't remove yet
                                discord_log_add("warning", discord_username, effective_name,
                                                "Has Member role but not found in alliance. May have renamed.")
                                member_info["ingame_name"] = effective_name
                                member_info["status"] = "not_found"
            else:
                # Member does NOT have the role - check if they should get it
                if in_alliance:
                    # Their nickname matches an alliance member - add role
                    try:
                        await dm.add_roles(member_role)
                        discord_log_add("role_added", discord_username, effective_name,
                                        "Nickname matches alliance member - auto-linked")
                        member_info["has_role"] = True
                        member_info["ingame_name"] = alliance_names.get(effective_name_lower, effective_name)
                        member_info["in_alliance"] = True
                        member_info["status"] = "in_alliance"

                        # Create/update link
                        key = effective_name_lower
                        if key not in links:
                            links[key] = []
                        if dm.id not in links[key]:
                            links[key].append(dm.id)
                            links_changed = True

                        discord_log_add("auto_linked", discord_username, effective_name,
                                        "Auto-linked by nickname match")
                    except discord.Forbidden:
                        discord_log_add("warning", discord_username, effective_name,
                                        "Could not add role (Forbidden)")
                else:
                    # No role, not in alliance — skip entirely
                    continue

            discord_data.append(member_info)

        if links_changed:
            save_links(links)

        # Add alliance members who are NOT on Discord yet
        matched_ingame = {d["ingame_name"].lower() for d in discord_data if d.get("ingame_name")}
        for name_lower, name_original in alliance_names.items():
            if name_lower not in matched_ingame:
                discord_data.append({
                    "discord_name": "-",
                    "discord_id": 0,
                    "ingame_name": name_original,
                    "nickname": "-",
                    "has_role": False,
                    "in_alliance": True,
                    "birded": False,
                    "status": "no_discord",
                })

        # Update shared data for dashboard
        set_discord_members_data(discord_data)

        _last_sync_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(f"[DISCORD] Sync complete at {_last_sync_time}")

    except Exception as e:
        print(f"[DISCORD] Sync error: {e}")
        import traceback
        traceback.print_exc()
        discord_log_add("warning", "Bot", "", f"Sync error: {e}")
    finally:
        _sync_running = False


@bot.event
async def on_ready():
    global _bot_ready, _bot_guild, GUILD_ID

    print(f"[DISCORD] Logged in as {bot.user} (ID: {bot.user.id})")

    # Auto-detect guild
    if bot.guilds:
        _bot_guild = bot.guilds[0]
        GUILD_ID = _bot_guild.id
        print(f"[DISCORD] Connected to guild: {_bot_guild.name} (ID: {GUILD_ID})")

    _bot_ready = True

    # Sync slash commands
    try:
        await bot.tree.sync()
        print("[DISCORD] Slash commands synced")
    except Exception as e:
        print(f"[DISCORD] Failed to sync slash commands: {e}")

    # Start periodic sync
    if not periodic_sync.is_running():
        periodic_sync.start()

    # Run initial sync
    if _bot_guild:
        await do_sync(_bot_guild)

    # Start Flask in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("[DISCORD] Flask dashboard started in background thread")


@tasks.loop(hours=1)
async def periodic_sync():
    """Run sync every hour."""
    if _bot_guild:
        await do_sync(_bot_guild)


@periodic_sync.before_loop
async def before_periodic_sync():
    await bot.wait_until_ready()


# Slash commands
@bot.tree.command(name="sync_now", description="Force an immediate sync of alliance members")
@app_commands.checks.has_permissions(manage_nicknames=True)
async def sync_now(interaction: discord.Interaction):
    global _sync_running
    if _sync_running:
        await interaction.response.send_message("A sync is already running. Please wait.", ephemeral=True)
        return
    await interaction.response.send_message("Starting sync...", ephemeral=True)
    await do_sync(interaction.guild)
    await interaction.followup.send("Sync complete!", ephemeral=True)


@bot.tree.command(name="status", description="Show bot status")
async def status(interaction: discord.Interaction):
    links = load_links()
    total_links = sum(len(v) for v in links.values())
    log_entries = len(discord_log_get())

    embed = discord.Embed(
        title="Bot Status",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Bot", value=f"{bot.user}", inline=True)
    embed.add_field(name="Guild", value=f"{_bot_guild.name if _bot_guild else 'N/A'}", inline=True)
    embed.add_field(name="Last Sync", value=_last_sync_time, inline=True)
    embed.add_field(name="Linked Players", value=str(len(links)), inline=True)
    embed.add_field(name="Total Links", value=str(total_links), inline=True)
    embed.add_field(name="Log Entries", value=str(log_entries), inline=True)
    embed.add_field(name="Sync Running", value="Yes" if _sync_running else "No", inline=True)

    await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------------
def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


@app.route("/")
def index():
    members = fetch_members()
    activity = fetch_activity()
    renames = fetch_renames()

    # Compute stats
    mights = [m.get("might_current", 0) or 0 for m in members]
    honors = [m.get("honor", 0) or 0 for m in members]
    loots = [m.get("loot_current", 0) or 0 for m in members]
    total_members = len(members)
    avg_might = int(sum(mights) / total_members) if total_members else 0
    max_might = max(mights) if mights else 0
    max_might_player = ""
    for m in members:
        if (m.get("might_current", 0) or 0) == max_might:
            max_might_player = m.get("player_name", "")
            break
    cum_might = sum(mights)
    avg_honor = int(sum(honors) / total_members) if total_members else 0
    cum_loot = sum(loots)

    stats = {
        "total_members": total_members,
        "avg_might": avg_might,
        "max_might": max_might,
        "max_might_player": max_might_player,
        "cum_might": cum_might,
        "avg_honor": avg_honor,
        "cum_loot": cum_loot,
    }

    # Filter renames for alliance member names
    member_names = {(m.get("name") or m.get("playerName") or "").lower() for m in members}
    member_ids = {m.get("playerId") or m.get("player_id") or m.get("id") for m in members}
    filtered_renames = []
    for r in renames:
        pid = r.get("playerId") or r.get("player_id") or r.get("id")
        old = (r.get("oldName") or r.get("old_name") or "").lower()
        new = (r.get("newName") or r.get("new_name") or "").lower()
        if pid in member_ids or old in member_names or new in member_names:
            filtered_renames.append(r)

    # Sort activity newest first, limit 50
    try:
        activity_sorted = sorted(activity, key=lambda x: x.get("date", x.get("timestamp", "")), reverse=True)[:50]
    except Exception:
        activity_sorted = activity[:50]

    # Event scores
    event_scores, loading = get_event_scores()

    event_names = [name for _, name in EVENTS]

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Discord data
    discord_members = get_discord_members_data()
    discord_activity = discord_log_get()[:50]

    return render_template(
        "index.html",
        members=members,
        stats=stats,
        activity=activity_sorted,
        renames=renames[:50],
        event_scores=event_scores,
        event_scores_loading=loading,
        event_names=event_names,
        last_updated=now_str,
        discord_members=discord_members,
        discord_activity=discord_activity,
        discord_bot_ready=_bot_ready,
        discord_last_sync=_last_sync_time,
    )


@app.route("/api/refresh")
def api_refresh():
    """AJAX endpoint for auto-refresh."""
    # Clear cache to force re-fetch
    _cache.clear()

    members = fetch_members()
    activity = fetch_activity()
    renames = fetch_renames()

    mights = [m.get("might_current", 0) or 0 for m in members]
    honors = [m.get("honor", 0) or 0 for m in members]
    loots = [m.get("loot_current", 0) or 0 for m in members]
    total_members = len(members)
    avg_might = int(sum(mights) / total_members) if total_members else 0
    max_might = max(mights) if mights else 0
    max_might_player = ""
    for m in members:
        if (m.get("might_current", 0) or 0) == max_might:
            max_might_player = m.get("player_name", "")
            break
    cum_might = sum(mights)
    avg_honor = int(sum(honors) / total_members) if total_members else 0
    cum_loot = sum(loots)

    try:
        activity_sorted = sorted(activity, key=lambda x: x.get("date", x.get("timestamp", "")), reverse=True)[:50]
    except Exception:
        activity_sorted = activity[:50]

    event_scores, loading = get_event_scores()

    return jsonify({
        "stats": {
            "total_members": total_members,
            "avg_might": fmt_num(avg_might),
            "max_might": fmt_num(max_might),
            "max_might_player": max_might_player,
            "cum_might": fmt_num(cum_might),
            "avg_honor": fmt_num(avg_honor),
            "cum_loot": fmt_num(cum_loot),
        },
        "members": members,
        "activity": activity_sorted,
        "renames": renames[:50],
        "event_scores": event_scores,
        "event_scores_loading": loading,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    })


@app.route("/api/discord")
def api_discord():
    """API endpoint returning Discord members data and activity log."""
    return jsonify({
        "discord_members": get_discord_members_data(),
        "discord_activity": discord_log_get(),
        "bot_ready": _bot_ready,
        "last_sync": _last_sync_time,
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
