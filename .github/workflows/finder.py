"""
NEET Telegram Channel Finder
Finds active NEET-related channels with 150-1500 subscribers,
whose admin/creator owns <=2 channels (within discovered dataset),
and sends results to your own Saved Messages daily.

SETUP:
1. pip install telethon
2. Get api_id/api_hash from https://my.telegram.org
3. Fill CONFIG below
4. First run: python finder.py  -> asks phone + OTP once, saves session file
5. Cron (VPS): 0 9 * * * /usr/bin/python3 /path/finder.py >> /path/finder.log 2>&1

LIMITATION: Telegram has no API to list "all channels a user administers"
globally. This script approximates by tracking, across ALL channels it has
ever discovered (stored in state.json), how many distinct channels share the
same creator. A creator with >2 known channels gets excluded. This is a
same-dataset heuristic, not a global truth.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.functions.channels import GetParticipantsRequest, GetFullChannelRequest
from telethon.tl.types import ChannelParticipantsAdmins, ChannelParticipantCreator

# ---------------- CONFIG ----------------
# Reads from env vars (set as GitHub Actions secrets). For local testing you
# can hardcode instead, but keep secrets out of committed code.
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

KEYWORDS = [
    "NEET", "NEET UG", "NEET aspirants", "NEET biology",
    "NEET preparation", "NEET 2027", "NEET notes", "NEET quiz",
]

MIN_SUBS = 150
MAX_SUBS = 1500
MAX_CHANNELS_PER_ADMIN = 2
INACTIVITY_DAYS = 14      # channel must have posted within this window to count as "active"
STATE_FILE = "state.json"
# -----------------------------------------


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"seen_channels": {}, "creator_channel_count": {}, "notified": []}


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
        # fallback: first admin if no explicit creator surfaced
        if participants.participants:
            return participants.participants[0].user_id
    except Exception:
        pass
    return None


async def search_keyword(client, keyword, state, results):
    res = await client(SearchRequest(q=keyword, limit=50))
    for chat in res.chats:
        if not getattr(chat, "broadcast", False):
            continue  # channels only, skip groups
        cid = str(chat.id)
        if cid in state["seen_channels"]:
            continue

        try:
            full = await client(GetFullChannelRequest(chat))
            subs = full.full_chat.participants_count
        except Exception:
            continue

        if not (MIN_SUBS <= subs <= MAX_SUBS):
            state["seen_channels"][cid] = {"skipped": "sub_count"}
            continue

        if not await is_active(client, chat):
            state["seen_channels"][cid] = {"skipped": "inactive"}
            continue

        creator_id = await get_creator_id(client, chat)
        creator_key = str(creator_id) if creator_id else f"unknown_{cid}"

        count = state["creator_channel_count"].get(creator_key, 0)
        if count >= MAX_CHANNELS_PER_ADMIN:
            state["seen_channels"][cid] = {"skipped": "admin_overloaded"}
            continue

        state["creator_channel_count"][creator_key] = count + 1
        state["seen_channels"][cid] = {
            "title": chat.title, "username": chat.username, "subs": subs,
            "creator": creator_key
        }
        results.append(chat)


async def main():
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    state = load_state()
    results = []

    for kw in KEYWORDS:
        try:
            await search_keyword(client, kw, state, results)
        except Exception as e:
            print(f"[warn] '{kw}' failed: {e}")
        await asyncio.sleep(2)  # avoid flood limits

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
        await client.send_message("me", "NEET channel finder: no new matches today.")

    save_state(state)
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
