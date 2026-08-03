"""
NEET Telegram Channel Finder
Crawls outward from seed channels via Telegram's "similar channels" graph,
joining channels and pulling their recommendations to discover more.
Filters candidates by: subscriber range, recent activity, admin channel
count, AND topical relevance (title/about must contain a NEET-related
keyword) before counting them as a match.

Runs multiple times a night (crawl-only, silently accumulating matches in
state.json). The final run of the morning sends everything accumulated
as one digest to your Saved Messages, guaranteeing a minimum batch size
if enough qualifying channels were found overnight.

LIMITATIONS:
- No API exists for "all channels a user administers" globally; admin count
  is a same-dataset heuristic tracked in state.json.
- Telegram does not publish a fixed daily API quota. Instead of a made-up
  90% number, this script processes large batches per run and backs off
  automatically the moment Telegram returns FloodWaitError -- that error IS
  the real ceiling. Joining ~100+ channels/day is still a genuine ban-risk;
  lower MAX_NEW_JOINS_PER_RUN / BATCHES_PER_RUN if your account gets
  restricted.
"""

import asyncio
import json
import os
import sys
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
SEND_DIGEST = os.environ.get("SEND_DIGEST", "false").lower() == "true"

SEED_CHANNELS = ["NeetQuizArena", "neetmarg"]

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
MAX_NEW_JOINS_PER_RUN = 25   # cap joins per run (ban-risk control)
JOIN_DELAY_SECONDS = 5
STATE_FILE = "state.json"
# -----------------------------------------


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            s = json.load(f)
    else:
        s = {}
    s.setdefault("seen_channels", {})
    s.setdefault("creator_channel_count", {})
    s.setdefault("notified", [])
    s.setdefault("joined", [])
    s.setdefault("queue", list(SEED_CHANNELS))
    s.setdefault("queued_set", list(SEED_CHANNELS))
    s.setdefault("pending", [])   # matches found but not yet sent
    return s


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def is_relevant(title, about):
    text = f"{title or ''} {about or ''}".lower()
    return any(kw in text for kw in NEET_KEYWORDS)


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
        print(f"[warn] flood wait {e.seconds}s -- this is Telegram's real ceiling, stopping joins")
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


async def process_channel(client, ref, state, joins_done):
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
            about = full.full_chat.about
        except Exception:
            return joins_done

        if not (MIN_SUBS <= subs <= MAX_SUBS):
            state["seen_channels"][cid] = {"skipped": "sub_count", "subs": subs}
        elif not is_relevant(chat.title, about):
            state["seen_channels"][cid] = {"skipped": "not_neet_related", "subs": subs}
        elif not await is_active(client, chat):
            state["seen_channels"][cid] = {"skipped": "inactive", "subs": subs}
        else:
            creator_id = await get_creator_id(client, chat)
            creator_key = str(creator_id) if creator_id else f"unknown_{cid}"
            count = state["creator_channel_count"].get(creator_key, 0)
            if count >= MAX_CHANNELS_PER_ADMIN:
                state["seen_channels"][cid] = {"skipped": "admin_overloaded", "subs": subs}
            else:
                state["creator_channel_count"][creator_key] = count + 1
                info = {
                    "id": cid, "title": chat.title, "username": chat.username,
                    "subs": subs, "creator": creator_key,
                }
                state["seen_channels"][cid] = info
                state["pending"].append(info)

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
    joins_done = 0
    batches_run = 0

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
                joins_done = await process_channel(client, ref, state, joins_done)
            except FloodWaitError as e:
                print(f"[warn] flood wait {e.seconds}s -- stopping run early")
                save_state(state)
                await client.send_message("me", "NEET finder: hit Telegram flood limit, stopping early for this run.") if SEND_DIGEST else None
                await client.disconnect()
                sys.exit(0)
            except Exception as e:
                print(f"[warn] '{ref}' failed: {e}")

        save_state(state)  # checkpoint after every batch

    print(f"Run summary: {batches_run} batches, {joins_done} joins, "
          f"{len(state['pending'])} pending matches, {len(state['queue'])} left in queue")

    if SEND_DIGEST:
        if state["pending"]:
            lines = [f"NEET channel finder — {len(state['pending'])} match(es):\n"]
            for info in state["pending"]:
                handle = f"@{info['username']}" if info["username"] else "(private/no username)"
                lines.append(f"• {info['title']} {handle} — {info['subs']} subs")
                state["notified"].append(info["id"])
            await client.send_message("me", "\n".join(lines))
            state["pending"] = []
        else:
            await client.send_message(
                "me",
                f"NEET channel finder: no matches accumulated overnight. "
                f"({len(state['queue'])} channels left in crawl queue)"
            )
        save_state(state)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
  
