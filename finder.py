"""
NEET Telegram Channel Finder + Cross-Promotion Outreach
=======================================================
Crawls outward from seed channels via Telegram's "similar channels" graph,
discovers small-but-active NEET channels, and (optionally) DMs their admins a
cross-promotion outreach message on behalf of @bioneettraps.

A candidate is a MATCH only if it passes ALL of:
  1. subscriber count within [MIN_SUBS, MAX_SUBS]
  2. topical relevance  (title/about contains a NEET keyword)
  3. recent activity     (posted within INACTIVITY_DAYS)
  4. admin de-duplication (the admin does not already run
     MAX_CHANNELS_PER_ADMIN matched channels)

Design notes / how this differs from a naive crawler
----------------------------------------------------
* We do NOT join channels just to read their recommendations --
  GetChannelRecommendationsRequest works on public channels without
  membership. We only JOIN channels that already passed the cheap filters,
  because joining is the single biggest ban-risk lever. This cuts joins by
  an order of magnitude versus join-everything crawlers.
* Admin de-duplication ("one person running 5 NEET channels") is resolved
  with the strongest signal available, in order:
      creator user-id (read from the admin list AFTER we join)  ->
      contact @handle parsed from the About text               ->
      unique fallback (no de-dup possible).
  The old code only ever tried the first, before joining, so it always
  failed and every channel looked like a unique admin.
* Outreach DMs are strictly opt-in (SEND_DMS=true), hard-capped per run,
  spaced out, de-duplicated, and abort instantly on PeerFloodError -- the
  clearest "you are being rate-limited / flagged" signal Telegram gives.

LIMITATIONS
-----------
* There is no API for "every channel a user administers" globally; admin
  count is a same-dataset heuristic tracked in state.json.
* Telegram publishes no fixed daily quota. We process bounded batches and
  back off the instant a FloodWaitError / PeerFloodError appears -- that
  error IS the real ceiling. Mass-joining and mass-DMing still risk account
  restriction; lower the *_PER_RUN caps if your account gets limited.
"""

import asyncio
import os
import re
import sys
import json
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError, PeerFloodError, UserAlreadyParticipantError,
    UserPrivacyRestrictedError, ChatWriteForbiddenError, RPCError,
)
from telethon.tl.functions.channels import (
    GetParticipantsRequest, GetFullChannelRequest,
    JoinChannelRequest, GetChannelRecommendationsRequest,
)
from telethon.tl.types import ChannelParticipantsAdmins, ChannelParticipantCreator

# ---------------- CONFIG ----------------
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

SEND_DIGEST = os.environ.get("SEND_DIGEST", "false").lower() == "true"
SEND_DMS = os.environ.get("SEND_DMS", "false").lower() == "true"

# The channel we are promoting. Matches whose admin runs this channel, and the
# channel itself, are never treated as outreach targets.
OWN_CHANNEL = os.environ.get("OWN_CHANNEL", "bioneettraps").lstrip("@").lower()
SEED_CHANNELS = ["NeetQuizArena", "neetmarg", OWN_CHANNEL]

NEET_KEYWORDS = [
    "neet", "mbbs", "medical entrance", "aiims", "biology", "physics",
    "chemistry", "pcb", "aspirant", "pre-medical", "ncert", "cbse bio",
    "medico", "botany", "zoology", "anatomy", "physiology",
]

MIN_SUBS = 150
MAX_SUBS = 1500
MAX_CHANNELS_PER_ADMIN = 2
INACTIVITY_DAYS = 14

MIN_DAILY_MATCHES = 5        # keep pulling batches until pending >= this (or queue/budget runs out)
BATCH_SIZE = 50
MAX_BATCHES_PER_RUN = 4      # cap total API load per run
MAX_NEW_JOINS_PER_RUN = 20   # cap joins per run (ban-risk control); we only join real candidates now
JOIN_DELAY_SECONDS = 5

# Outreach (DM) controls -- deliberately conservative; DMing strangers is the
# highest ban-risk action on Telegram.
MAX_DMS_PER_RUN = int(os.environ.get("MAX_DMS_PER_RUN", "8"))
DM_DELAY_SECONDS = int(os.environ.get("DM_DELAY_SECONDS", "45"))

DEFAULT_OUTREACH = (
    "Hi! 👋 I run @{own} (MedicNeet — NEET CBT tests & daily practice questions). "
    "I came across *{title}* and really like what you're building for NEET aspirants. "
    "Our audiences overlap a lot, so I'd love to do a small, free cross-promotion — "
    "a mutual shoutout where we each recommend the other's channel to our members. "
    "No cost, just two channels helping each other grow. Would you be open to it? 🙌"
)
OUTREACH_MESSAGE = os.environ.get("OUTREACH_MESSAGE", DEFAULT_OUTREACH)

STATE_FILE = "state.json"

# @handle regex for parsing admin contacts out of About text.
HANDLE_RE = re.compile(r"@([A-Za-z0-9_]{4,32})")
# -----------------------------------------


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            s = json.load(f)
    else:
        s = {}
    s.setdefault("seen_channels", {})       # cid -> match info OR {"skipped": reason}
    s.setdefault("creator_channel_count", {})  # admin identity -> #matched channels
    s.setdefault("notified", [])            # cids included in a sent digest
    s.setdefault("joined", [])              # cids we have joined
    s.setdefault("queue", list(SEED_CHANNELS))
    s.setdefault("queued_set", list(SEED_CHANNELS))
    s.setdefault("pending", [])             # matches found but not yet sent in a digest
    s.setdefault("dmed", [])                # cids we have successfully DM'd
    s.setdefault("dm_failed", {})           # cid -> reason we could not DM
    return s


def save_state(state):
    # Write atomically so a crash mid-write can't corrupt accumulated state.
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    os.replace(tmp, STATE_FILE)


def is_relevant(title, about):
    text = f"{title or ''} {about or ''}".lower()
    return any(kw in text for kw in NEET_KEYWORDS)


def contact_handles(about):
    """@handles in About, minus obvious bots and our own channel -- used both
    as a de-dup signal and as a fallback outreach target."""
    out = []
    for h in HANDLE_RE.findall(about or ""):
        hl = h.lower()
        if hl == OWN_CHANNEL or hl.endswith("bot"):
            continue
        if hl not in out:
            out.append(hl)
    return out


async def is_active(client, entity):
    try:
        async for msg in client.iter_messages(entity, limit=1):
            return msg.date >= datetime.now(timezone.utc) - timedelta(days=INACTIVITY_DAYS)
    except RPCError as e:
        print(f"[warn] activity check failed for {getattr(entity, 'username', entity.id)}: {e}")
    return False


async def get_creator(client, entity):
    """Return the creator User object (needs membership for most channels).
    Falls back to the first admin if no explicit creator is exposed."""
    try:
        res = await client(GetParticipantsRequest(
            channel=entity, filter=ChannelParticipantsAdmins(),
            offset=0, limit=50, hash=0,
        ))
        users = {u.id: u for u in res.users}
        for p in res.participants:
            if isinstance(p, ChannelParticipantCreator):
                return users.get(p.user_id)
        if res.participants:
            return users.get(res.participants[0].user_id)
    except RPCError as e:
        print(f"[warn] admin lookup failed for {getattr(entity, 'username', entity.id)}: {e}")
    return None


def admin_identity(cid, creator, about):
    """Strongest available admin key for de-duplication."""
    if creator is not None:
        return f"user_{creator.id}"
    handles = contact_handles(about)
    if handles:
        return f"contact_{handles[0]}"
    return f"unknown_{cid}"


async def safe_join(client, chat, state):
    cid = str(chat.id)
    if cid in state["joined"]:
        return True
    if state["_joins_done"] >= MAX_NEW_JOINS_PER_RUN:
        return False
    try:
        await client(JoinChannelRequest(chat))
        state["joined"].append(cid)
        state["_joins_done"] += 1
        await asyncio.sleep(JOIN_DELAY_SECONDS)
        return True
    except UserAlreadyParticipantError:
        state["joined"].append(cid)
        return True
    except FloodWaitError as e:
        print(f"[warn] flood wait {e.seconds}s on join -- Telegram's real ceiling, stopping joins")
        state["_joins_done"] = MAX_NEW_JOINS_PER_RUN  # block further joins this run
        return False
    except RPCError as e:
        print(f"[warn] join failed for {getattr(chat, 'username', cid)}: {e}")
        return False


async def get_similar(client, chat):
    try:
        res = await client(GetChannelRecommendationsRequest(channel=chat))
        return [c for c in res.chats if getattr(c, "broadcast", False)]
    except RPCError as e:
        print(f"[warn] recommendations failed for {getattr(chat, 'username', chat.id)}: {e}")
        return []


def enqueue_similar(state, sims):
    added = 0
    for sim in sims:
        key = (sim.username or str(sim.id)).lower()
        if key in state["_queued"] or str(sim.id) in state["seen_channels"]:
            continue
        state["queue"].append(sim.username or str(sim.id))
        state["_queued"].add(key)
        added += 1
    return added


async def send_outreach(client, chat, creator, about, state):
    """DM the channel's admin a cross-promo message. Returns True on success.
    Prefers the resolved creator; falls back to @handles in the About text."""
    cid = str(chat.id)
    if cid in state["dmed"]:
        return True
    if state["_dms_done"] >= MAX_DMS_PER_RUN:
        return False

    msg = OUTREACH_MESSAGE.format(own=OWN_CHANNEL, title=chat.title or "your channel")

    # Build ordered list of DM targets: creator first, then About @handles.
    targets = []
    if creator is not None and not getattr(creator, "is_self", False) and not creator.bot:
        targets.append(creator)
    for h in contact_handles(about):
        targets.append(h)

    if not targets:
        state["dm_failed"][cid] = "no_contact"
        return False

    for target in targets:
        try:
            entity = target if not isinstance(target, str) else await client.get_entity(target)
            if getattr(entity, "id", None) == state["_me_id"]:
                continue
            await client.send_message(entity, msg, link_preview=False)
            state["dmed"].append(cid)
            state["dm_failed"].pop(cid, None)
            state["_dms_done"] += 1
            label = f"@{entity.username}" if getattr(entity, "username", None) else entity.id
            print(f"[dm] sent outreach for '{chat.title}' -> {label}")
            await asyncio.sleep(DM_DELAY_SECONDS)
            return True
        except PeerFloodError:
            print("[warn] PeerFloodError -- account is being rate-limited; stopping ALL DMs this run")
            state["_dms_done"] = MAX_DMS_PER_RUN
            state["dm_failed"][cid] = "peer_flood"
            return False
        except (UserPrivacyRestrictedError, ChatWriteForbiddenError):
            state["dm_failed"][cid] = "privacy"
            continue  # try next target
        except FloodWaitError as e:
            print(f"[warn] flood wait {e.seconds}s on DM -- stopping DMs this run")
            state["_dms_done"] = MAX_DMS_PER_RUN
            state["dm_failed"][cid] = "flood_wait"
            return False
        except RPCError as e:
            state["dm_failed"][cid] = f"error:{type(e).__name__}"
            continue

    return False


# Failure reasons that won't change on a retry -- don't keep re-hitting the API.
PERMANENT_DM_FAILURES = {"no_contact", "privacy"}


def outreach_targets(state):
    """Every matched channel we could still reach out to, best-match first.
    Pulls from both the current 'pending' list and the historical matches in
    'seen_channels', so already-accumulated matches get contacted too."""
    # (cid, info) pairs from both sources; seen_channels is keyed by cid, and
    # older records may not carry "id" inside the value.
    candidates = [(info.get("id"), info) for info in state["pending"]]
    candidates += list(state["seen_channels"].items())

    picked = {}
    for cid, info in candidates:
        if not isinstance(info, dict) or "skipped" in info:
            continue
        cid = cid or info.get("id")
        if not cid:
            continue
        if (info.get("username") or "").lower() == OWN_CHANNEL:
            continue
        if cid in state["dmed"]:
            continue
        if state["dm_failed"].get(cid) in PERMANENT_DM_FAILURES:
            continue
        picked[cid] = {**info, "id": cid}
    # Best match for a mutual shoutout = smallest active channels first
    # (they benefit most and reciprocate most readily).
    return sorted(picked.values(), key=lambda i: i.get("subs", 10**9))


async def outreach_pass(client, state):
    """Resolve each matched channel's admin and DM the outreach message.
    Runs on both freshly-found and previously-accumulated matches."""
    targets = outreach_targets(state)
    if not targets:
        print("[dm] no eligible outreach targets")
        return
    print(f"[dm] {len(targets)} eligible target(s); cap {MAX_DMS_PER_RUN} this run")

    for info in targets:
        if state["_dms_done"] >= MAX_DMS_PER_RUN:
            break
        ref = info.get("username") or info["id"]
        try:
            chat = await client.get_entity(ref)
        except (ValueError, RPCError) as e:
            print(f"[warn] outreach resolve failed for {ref}: {e}")
            continue
        try:
            full = await client(GetFullChannelRequest(chat))
            about = full.full_chat.about
        except RPCError:
            about = " ".join(f"@{h}" for h in info.get("contacts", []))
        # Membership (no-op / no join budget spent if already joined) lets us
        # read the creator for a direct DM.
        joined = await safe_join(client, chat, state)
        creator = await get_creator(client, chat) if joined else None
        await send_outreach(client, chat, creator, about, state)


async def reseed_queue(client, state):
    """When the crawl queue is empty, re-pull recommendations from our matched
    channels and seeds. Telegram rotates recommendations, so this surfaces
    channels we haven't seen yet -- without reprocessing seen ones."""
    hubs = list(SEED_CHANNELS)
    for info in state["seen_channels"].values():
        if isinstance(info, dict) and "skipped" not in info and info.get("username"):
            hubs.append(info["username"])

    added = 0
    for ref in dict.fromkeys(hubs):  # de-dup, preserve order
        try:
            chat = await client.get_entity(ref)
        except (ValueError, RPCError):
            continue
        if not getattr(chat, "broadcast", False):
            continue
        added += enqueue_similar(state, await get_similar(client, chat))
    print(f"[reseed] queue was empty; pulled {added} fresh channel(s) from {len(hubs)} hubs")


async def process_channel(client, ref, state):
    try:
        chat = ref if hasattr(ref, "id") else await client.get_entity(ref)
    except (ValueError, RPCError):
        return

    if not getattr(chat, "broadcast", False):
        return
    cid = str(chat.id)

    # Already evaluated (and already expanded) -> nothing to do.
    if cid in state["seen_channels"]:
        return

    # Never target our own channel.
    if (chat.username or "").lower() == OWN_CHANNEL:
        state["seen_channels"][cid] = {"skipped": "own_channel"}
        return

    try:
        full = await client(GetFullChannelRequest(chat))
        subs = full.full_chat.participants_count
        about = full.full_chat.about
    except RPCError as e:
        print(f"[warn] full-channel fetch failed for {getattr(chat, 'username', cid)}: {e}")
        return

    # Expand the crawl graph from on-topic hubs only (no join required). Doing
    # this before the pass/fail decision keeps the crawl broad but focused.
    if is_relevant(chat.title, about):
        enqueue_similar(state, await get_similar(client, chat))

    # ---- Cheap filters (no join) ----
    if not (MIN_SUBS <= subs <= MAX_SUBS):
        state["seen_channels"][cid] = {"skipped": "sub_count", "subs": subs}
        return
    if not is_relevant(chat.title, about):
        state["seen_channels"][cid] = {"skipped": "not_neet_related", "subs": subs}
        return
    if not await is_active(client, chat):
        state["seen_channels"][cid] = {"skipped": "inactive", "subs": subs}
        return

    # ---- Passed cheap filters: now join so we can read admins ----
    joined = await safe_join(client, chat, state)

    creator = await get_creator(client, chat) if joined else None
    identity = admin_identity(cid, creator, about)
    count = state["creator_channel_count"].get(identity, 0)
    if count >= MAX_CHANNELS_PER_ADMIN:
        state["seen_channels"][cid] = {"skipped": "admin_overloaded", "subs": subs, "creator": identity}
        return

    # ---- MATCH ----
    state["creator_channel_count"][identity] = count + 1
    info = {
        "id": cid, "title": chat.title, "username": chat.username,
        "subs": subs, "creator": identity,
        "contacts": contact_handles(about),
    }
    state["seen_channels"][cid] = info
    state["pending"].append(info)
    print(f"[match] {chat.title} (@{chat.username}) — {subs} subs, admin={identity}")


async def main():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    me = await client.get_me()

    state = load_state()
    # In-memory O(1) lookup mirrors + per-run counters (underscore keys are
    # never persisted; stripped before save).
    state["_queued"] = {q.lower() for q in state["queued_set"]}
    state["_joins_done"] = 0
    state["_dms_done"] = 0
    state["_me_id"] = me.id

    if not state["queue"]:
        await reseed_queue(client, state)

    batches_run = 0
    try:
        while (
            batches_run < MAX_BATCHES_PER_RUN
            and state["queue"]
            and len(state["pending"]) < MIN_DAILY_MATCHES
        ):
            batch = state["queue"][:BATCH_SIZE]
            state["queue"] = state["queue"][BATCH_SIZE:]
            batches_run += 1

            for ref in batch:
                try:
                    await process_channel(client, ref, state)
                except FloodWaitError as e:
                    print(f"[warn] flood wait {e.seconds}s -- stopping run early")
                    if SEND_DIGEST:
                        await client.send_message(
                            "me", "NEET finder: hit Telegram flood limit, stopping early this run."
                        )
                    _persist(state)
                    await client.disconnect()
                    sys.exit(0)
                except Exception as e:  # never let one bad channel kill the batch
                    print(f"[warn] '{ref}' failed: {type(e).__name__}: {e}")

            _persist(state)  # checkpoint after every batch

        # Keep queued_set from drifting: rebuild it from the durable in-memory set.
        state["queued_set"] = sorted(state["_queued"])

        if SEND_DMS:
            await outreach_pass(client, state)
            _persist(state)

        print(
            f"Run summary: {batches_run} batches | {state['_joins_done']} joins | "
            f"{state['_dms_done']} DMs | {len(state['pending'])} pending | "
            f"{len(state['queue'])} left in queue"
        )

        if SEND_DIGEST:
            await send_digest(client, state)

        _persist(state)
    finally:
        await client.disconnect()


async def send_digest(client, state):
    if not state["pending"]:
        await client.send_message(
            "me",
            f"NEET channel finder: no new matches accumulated. "
            f"({len(state['queue'])} channels left in crawl queue)"
        )
        return

    lines = [f"NEET channel finder — {len(state['pending'])} match(es):\n"]
    for info in state["pending"]:
        handle = f"@{info['username']}" if info.get("username") else "(private/no username)"
        dm_note = ""
        if info["id"] in state["dmed"]:
            dm_note = "  ✅ DM sent"
        elif info["id"] in state["dm_failed"]:
            dm_note = f"  ⚠️ DM: {state['dm_failed'][info['id']]}"
        lines.append(f"• {info['title']} {handle} — {info['subs']} subs{dm_note}")
        state["notified"].append(info["id"])
    await client.send_message("me", "\n".join(lines))
    state["pending"] = []


def _persist(state):
    """Strip transient underscore keys, then save."""
    transient = {k: state.pop(k) for k in list(state) if k.startswith("_")}
    try:
        save_state(state)
    finally:
        state.update(transient)


if __name__ == "__main__":
    asyncio.run(main())
