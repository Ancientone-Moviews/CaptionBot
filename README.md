# 🎬 MediaInfo Bot

> A Telegram bot that automatically enriches video captions with media information — quality, codec, duration, audio languages, and subtitles — for posts in your channels. Also works in private chats for on-demand analysis.

---

## Features

- **Smart partial scanning** — streams as little as 16 KB first, escalates to 256 KB, then full download only as a last resort
- **Multi-Bot Architecture (Helper Bots)** — Routes tasks to specialized helper bots to eliminate cross-DC auth errors and mitigate `FLOOD_WAIT`s.
- **MongoDB Persistent Queue** — Uses MongoDB to store pending message queues and bot statistics, preventing data loss during restarts and flood cooldowns.
- **Intelligent Cooldowns** — Avoids severe `FLOOD_WAIT` penalties using minimum edit delays, exponential backoff, and periodic cooldowns.
- **Automated Updates** — Remote updates by pulling from an upstream GitHub repository on bot restart.
- **Parallel processing** — background queue processors for channels
- **Resolution detection** — 240p through 2160p+
- **Codec detection** — x264 / HEVC / AV1 / VP9 / MPEG4 with bit depth (e.g. `10bit`)
- **HDR detection** — HDR and Dolby Vision flags from stream metadata
- **Audio language labelling** — 50+ language codes resolved to full names; unknown streams labelled `Unknown`
- **Subtitle detection** — PGS, SRT, ASS/SSA, and more; falls back to `No Sub`
- **Channel mode** — auto-edits post captions; skips posts already containing media info
- **Private chat mode** — replies with a live progress message updated in real time
- **Admin commands** — server stats, restart, git pull + restart, shutdown
- **Scheduled garbage collection** — keeps memory use low over long uptimes

---

## Caption Format

```
<original caption or filename>

🎬 1080p HEVC 10bit HDR | ⏳ 01:45:32
🔊 English, Hindi
💬 ESUB
```

The template is fully customisable via the `CAPTION_TEMPLATE` environment variable using Python `str.format()` placeholders:

| Placeholder | Description |
|---|---|
| `{title}` | Original caption or filename |
| `{video_line}` | Quality + codec + bit depth + HDR (e.g. `1080p HEVC 10bit HDR`) |
| `{duration}` | Duration as `HH:MM:SS` |
| `{audio}` | Comma-separated audio language list |
| `{subtitle}` | Subtitle languages, or `No Sub` |

---

## Requirements

- Python 3.10+
- `ffprobe` (part of [FFmpeg](https://ffmpeg.org/)) — auto-installed on first run if missing
- `mediainfo` — auto-installed on first run if missing
- MongoDB Database (optional, but highly recommended for queue persistence)
- Telegram API credentials from [my.telegram.org](https://my.telegram.org)
- A bot token from [@BotFather](https://t.me/BotFather)
- The bot must be an **admin** in every target channel (to edit captions)

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/youruser/mediainfo-bot.git
cd mediainfo-bot
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a `.env` file in the project root (you can copy the example below):

```env
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_user_id
ALLOWED_CHATS=-1001234567890,-1009876543210

# New variables
MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
MONGO_DB_NAME=mediainfo_bot
UPSTREAM_REPO=https://github.com/youruser/mediainfo-bot
UPSTREAM_BRANCH=main
HELPER_TOKENS=token1 token2 token3
```

Full reference:

| Variable | Description | Default |
|---|---|---|
| `API_ID` | Telegram API ID from my.telegram.org | — |
| `API_HASH` | Telegram API hash from my.telegram.org | — |
| `BOT_TOKEN` | Bot token from @BotFather | — |
| `ADMIN_ID` | Your Telegram user ID (for admin commands) | — |
| `ALLOWED_CHATS` | Comma-separated channel IDs (e.g. `-1001234567890,-1009876543210`) | — |
| `LOG_LEVEL` | Logging level (`INFO`, `DEBUG`, `WARNING`, etc.) | `INFO` |
| `LOG_FORMAT` | Log line format string | timestamp/module/level/message |
| `GC_THRESHOLD_0` | Python GC generation 0 threshold | `500` |
| `GC_THRESHOLD_1` | Python GC generation 1 threshold | `5` |
| `GC_THRESHOLD_2` | Python GC generation 2 threshold | `5` |
| `CAPTION_TEMPLATE` | Custom HTML caption template (see above) | built-in |
| `MONGO_URI` | MongoDB connection URI for persistent queues | — |
| `MONGO_DB_NAME` | MongoDB database name | `mediainfo_bot` |
| `UPSTREAM_REPO` | GitHub repository URL for auto-updating | — |
| `UPSTREAM_BRANCH` | GitHub branch for auto-updating | `main` |
| `HELPER_TOKENS` | Space-separated list of helper bot tokens | — |

### 4. Run

```bash
python bot.py
```

On startup the bot will:
1. Install `ffmpeg` and `mediainfo` if not present
2. Connect to Telegram
3. Send a `🚀 Bot Started` message to `ADMIN_ID`
4. Begin scheduling garbage collection every 10 minutes

---

## Docker

```bash
docker build -t mediainfo-bot .
docker run -d --env-file .env mediainfo-bot
```

The image is based on `python:3.11-slim` and installs `ffmpeg` at build time.

---

## Deploy to Railway / Heroku

Set all environment variables in the platform dashboard and push. The `Procfile` is already included.

---

## How It Works

```
Post arrives in channel (or file sent in private chat)
        │
        ▼
Already has media info? ──► Skip (channel) / reply anyway (private)
        │
        ▼
Stream first 16 KB → run mediainfo
        │
  width & height found?
    Yes ──► Build caption
    No  ──► Retry once
        │
        ▼
Stream first 256 KB → run mediainfo
        │
  width & height found?
    Yes ──► Build caption
    No  ──►
        │
        ▼
Full download → run mediainfo → Build caption
        │
        ▼
Channel: edit_caption()   Private: edit progress message
        │
        ▼
Temp files cleaned up
```

---

## Commands

### User Commands

| Command | Description |
|---|---|
| `/start` | Introduction and usage guide |
| `/info` (reply to video) | Analyse a specific video inline |

### Admin Commands

| Command | Description |
|---|---|
| `/server` | Show CPU, RAM, and disk usage |
| `/setup <chat_id>` | Add all helper bots as admins to the target channel |
| `/restart` | Restart the bot process (`os.execv`) |
| `/update` | `git pull` + `pip install -r requirements.txt` + restart |
| `/shutdown` | Stop the scheduler and bot, then exit |

---

## Supported Audio Languages

The bot resolves ISO 639-1 and ISO 639-2 language codes to full names. Recognised languages include:

Hindi, Marathi, Tamil, Telugu, Malayalam, Kannada, Bengali, Gujarati, Punjabi, Bhojpuri, Urdu, English, French, German, Spanish, Portuguese, Italian, Dutch, Polish, Swedish, Danish, Norwegian, Finnish, Russian, Ukrainian, Bulgarian, Czech, Slovak, Serbian, Croatian, Romanian, Hungarian, Greek, Turkish, Arabic, Hebrew, Persian, Chinese, Japanese, Korean, Thai, Vietnamese, Indonesian, Malay, Tagalog, and more.

Unknown codes are labelled `Unknown`.

---

## Project Structure

```
mediainfo-bot/
├── bot.py            # Main bot logic — handlers, media processing, caption building
├── config.py         # Environment variable loading with defaults
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container build (python:3.11-slim + ffmpeg)
├── Procfile          # For Railway / Heroku deployments
└── .gitignore
```

---

## Dependencies

| Package | Purpose |
|---|---|
| `pyrofork` | Telegram MTProto client (Pyrogram fork) |
| `TgCrypto` | Fast cryptography for Pyrogram |
| `aiofiles` | Async file I/O for streaming downloads |
| `apscheduler` | Periodic garbage collection scheduler |
| `psutil` | CPU / RAM / disk stats for `/server` |
| `mediainfo` (system) | Stream metadata extraction |
| `ffmpeg` / `ffprobe` (system) | Fallback media analysis |

---