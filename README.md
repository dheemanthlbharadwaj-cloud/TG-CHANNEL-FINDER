# TG-CHANNEL-FINDER

Finds **small, active, NEET-focused** Telegram channels that are good
cross-promotion partners for [@bioneettraps](https://t.me/bioneettraps), and
(optionally) DMs their admins an outreach message.

## How it works

Starting from a few seed channels, it walks Telegram's *"similar channels"*
recommendation graph. A channel becomes a **match** only if it passes every
filter:

| Filter            | Rule                                                        |
| ----------------- | ----------------------------------------------------------- |
| Subscribers       | between `MIN_SUBS` (150) and `MAX_SUBS` (1500)              |
| Relevance         | title/about contains a NEET keyword                         |
| Activity          | posted within the last `INACTIVITY_DAYS` (14)              |
| Admin de-dup      | the admin doesn't already run `MAX_CHANNELS_PER_ADMIN` (2) matched channels |

State (everything seen/joined/matched/DM'd) is persisted in `state.json`, so
runs accumulate and never repeat work or double-send a DM.

### Design choices worth knowing

- **We don't join channels just to read them.** Recommendations are fetched
  without membership; we only *join* channels that already passed the cheap
  filters. Joining is the biggest ban-risk lever, so this keeps joins low.
- **Admin de-duplication actually works now.** The admin identity is resolved
  as, in order: the creator's user-id (read from the admin list *after*
  joining) → a contact `@handle` parsed from the About text → a unique
  fallback. This is what powers the "filter out one person running many
  channels" rule.
- **Outreach is a separate pass** over *all* matched channels (freshly found
  **and** previously accumulated), smallest-first, so your existing matches
  get contacted too.

## Setup

1. Create an app at <https://my.telegram.org> to get `API_ID` / `API_HASH`.
2. Generate a session string **on your own machine** (never in CI):

   ```bash
   pip install -r requirements.txt
   # fill API_ID / API_HASH into generate_session.py, then:
   python generate_session.py
   ```

3. Add three GitHub Actions **secrets**: `TG_API_ID`, `TG_API_HASH`,
   `TG_SESSION_STRING`.

## Running

Scheduled runs (nightly, `23:30 UTC`) **crawl silently** — they discover and
accumulate matches but send nothing. To act, trigger the workflow manually
(**Actions → NEET Channel Finder → Run workflow**) and tick the inputs:

- **`send_digest`** — DM yourself (Saved Messages) the list of matches, each
  annotated with its outreach status.
- **`send_dms`** — send the cross-promotion outreach message to matched
  channel admins.

You can also run locally with environment variables:

```bash
TG_API_ID=... TG_API_HASH=... TG_SESSION_STRING=... \
SEND_DMS=true SEND_DIGEST=true python finder.py
```

## Outreach controls (env vars)

| Variable            | Default | Meaning                                              |
| ------------------- | ------- | ---------------------------------------------------- |
| `SEND_DMS`          | `false` | Actually send outreach DMs                           |
| `MAX_DMS_PER_RUN`   | `8`     | Hard cap on DMs per run (ban-risk control)           |
| `DM_DELAY_SECONDS`  | `45`    | Spacing between DMs                                   |
| `OWN_CHANNEL`       | `bioneettraps` | Channel being promoted; never contacted itself |
| `OUTREACH_MESSAGE`  | built-in | Message template; supports `{own}` and `{title}`    |

**Ban-risk note:** mass-DMing strangers is the riskiest thing a Telegram
account can do. The script hard-caps DMs, spaces them out, de-duplicates, and
aborts all DMs the instant Telegram returns `PeerFloodError`. Start with a low
`MAX_DMS_PER_RUN` on a warmed-up account and raise it slowly.

## Files

- `finder.py` — the crawler + outreach engine.
- `generate_session.py` — one-time login to mint a session string.
- `.github/workflows/finder.yml` — schedule + manual-dispatch runner.
- `state.json` — accumulated state (committed back after each run).
