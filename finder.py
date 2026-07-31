"""
NEET Telegram Channel Finder
Crawls outward from seed channels using Telegram's "similar channels"
recommendation graph: joins a seed, pulls its similar-channels list, filters
each candidate (150-1500 subs, active, admin owns <=2 known channels), joins
qualifying/unexplored ones so the crawl can continue from them next run, and
sends new matches to your own Saved Messages.

SETUP:
1. pip install telethon
2. Get api_id/api_hash from https://my.telegram.org
3. Generate a session string once (see generate_session.py)
4. Fill CONFIG / env vars below
5. Run: python finder.py

LIMITATIONS:
- Telegram has no API for "all channels a user administers" globally. Admin
  count is a same-dataset heuristic tracked in state.json, not global truth.
- Joining channels is rate-limited by Telegram. MAX_NEW_JOINS_PER_RUN caps
  joins per run to reduce flood-wait / spam-flag risk. The crawl frontier
  (queue) persists in state.json so it continues where it left off each day.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import (
    GetParticipantsRequest, GetFullChannelRequest,
    JoinChannelRequest, GetChannelRecommendationsRequest,
)
from telethon.tl.types import ChannelParticipantsAdmins, ChannelParticipantCreator

# ---------------- CONFIG ----------------
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

SEED_CHANNELS = ["NeetQuizArena", "neetmarg"]

MIN_SUBS = 150
MAX_SUBS = 1500
MAX_CHANNELS_PER_ADMIN = 2
INACTIVITY_DAYS = 14
MAX_NEW_JOINS_PER_RUN = 15   # safety cap: avoid flood-wait / spam flags
JOIN_DELAY_SECONDS = 8       # pause between joins
STATE_FILE = "state.json"
# -----------------------------------------


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            s = json.load(f)
    else:
        s = {}
    s.setdefault("seen_channels", {})       # channel id -> info
    s.setdefault("creator_channel_count", {})
    s.setdefault("notified", [])
    s.setdefault("joined", [])              # channel ids we've joined
    s.setdefault("queue", list(SEED_CHANNELS))   # usernames/ids to crawl
    s.setdefault("queued_set", list(SEED_CHANNELS))  # dedupe helper
    return s


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


async def is_active(client, entity):
    async for msg in client.iter_messages(entity, limit=1):
        if msg.date >= datetime.now(timezone.utc) - timedelta(days=INACTIVITY_DAYS):
            return True
    return False


async def get_creator_id(client, entity):
    try:
        participants = await client(GetParticipantsRequest(
            channel=entity, filter=ChannelParticipantsAdmins(),
            offset=0, limit=50, hash=0
        ))
        for p in participants.participants:
            if isinstance(p, ChannelParticipantCreator):
                return p.user_id
        if participants.participants:
            return participants.participants[0].user_id
    except Exception:
        pass
    return None


async def safe_join(client, chat, state, joins_done):
    """Join a channel, respecting the per-run join cap and flood waits."""
    cid = str(chat.id)
    if cid in state["joined"]:
        return joins_done, True
    if joins_done >= MAX_NEW_JOINS_PER_RUN:
        return joins_done, False
    try:
        await client(JoinChannelRequest(chat))
        state["joined"].append(cid)
        joins_done += 1
        await asyncio.sleep(JOIN_DELAY_SECONDS)
        return joins_done, True
    except UserAlreadyParticipantError:
        state["joined"].append(cid)
        return joins_done, True
    except FloodWaitError as e:
        print(f"[warn] flood wait {e.seconds}s, stopping joins for this run")
        return joins_done, False
    except Exception as e:
        print(f"[warn] join failed for {getattr(chat, 'username', cid)}: {e}")
        return joins_done, False


async def get_similar(client, chat):
    try:
        res = await client(GetChannelRecommendationsRequest(channel=chat))
        return [c for c in res.chats if getattr(c, "broadcast", False)]
    except Exception as e:
        print(f"[warn] recommendations failed for {getattr(chat, 'username', chat.id)}: {e}")
        return []


async def process_channel(client, ref, state, results, joins_done):
    """ref is a username string or a resolved chat entity."""
    try:
        chat = ref if hasattr(ref, "id") else await client.get_entity(ref)
    except Exception:
        return joins_done

    if not getattr(chat, "broadcast", False):
        return joins_done
    cid = str(chat.id)

    if cid not in state["seen_channels"]:
        try:
            full = await client(GetFullChannelRequest(chat))
            subs = full.full_chat.participants_count
        except Exception:
            return joins_done

        qualifies = MIN_SUBS <= subs <= MAX_SUBS
        active = qualifies and await is_active(client, chat)

        if not qualifies:
            state["seen_channels"][cid] = {"skipped": "sub_count", "subs": subs}
        elif not active:
            state["seen_channels"][cid] = {"skipped": "inactive", "subs": subs}
        else:
            creator_id = await get_creator_id(client, chat)
            creator_key = str(creator_id) if creator_id else f"unknown_{cid}"
            count = state["creator_channel_count"].get(creator_key, 0)
            if count >= MAX_CHANNELS_PER_ADMIN:
                state["seen_channels"][cid] = {"skipped": "admin_overloaded", "subs": subs}
            else:
                state["creator_channel_count"][creator_key] = count + 1
                state["seen_channels"][cid] = {
                    "title": chat.title, "username": chat.username,
                    "subs": subs, "creator": creator_key,
                }
                results.append(chat)

    # Join (if capacity allows) so we can pull its recommendations too,
    # and to keep crawling outward from it in this or a future run.
    joins_done, joined = await safe_join(client, chat, state, joins_done)
    if joined:
        for sim in await get_similar(client, chat):
            uname = sim.username or str(sim.id)
            if uname not in state["queued_set"] and str(sim.id) not in state["seen_channels"]:
                state["queue"].append(uname)
                state["queued_set"].append(uname)

    return joins_done


async def main():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    state = load_state()
    results = []
    joins_done = 0

    # process a bounded slice of the queue per run so it doesn't run forever
    batch = state["queue"][:40]
    state["queue"] = state["queue"][40:]

    for ref in batch:
        try:
            joins_done = await process_channel(client, ref, state, results, joins_done)
        except Exception as e:
            print(f"[warn] '{ref}' failed: {e}")

    new = [c for c in results if str(c.id) not in state["notified"]]

    if new:
        lines = [f"NEET channel finder — {len(new)} new match(es):\n"]
        for c in new:
            info = state["seen_channels"][str(c.id)]
            handle = f"@{c.username}" if c.username else "(private/no username)"
            lines.append(f"• {c.title} {handle} — {info['subs']} subs")
            state["notified"].append(str(c.id))
        await client.send_message("me", "\n".join(lines))
    else:
        await client.send_message(
            "me",
            f"NEET channel finder: no new matches today. "
            f"({len(state['queue'])} channels left in crawl queue)"
        )

    save_state(state)
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())