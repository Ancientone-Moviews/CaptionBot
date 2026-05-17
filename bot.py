import asyncio
import json
import subprocess
import os
import logging
import sys
import psutil
import gc
import re
import uuid
import time
import random
from aiofiles import open as aiopen
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ChatType
from functools import lru_cache
from typing import Optional
from aiofiles.os import remove as aioremove
from pyrogram.errors import MessageNotModified, FloodWait
from collections import defaultdict
from motor.motor_asyncio import AsyncIOMotorClient
from config import (
    API_ID, API_HASH, BOT_TOKEN,
    ADMIN_ID, ALLOWED_CHATS,
    LOG_FORMAT, LOG_LEVEL,
    GC_THRESHOLD,
    CAPTION_TEMPLATE,
    MONGO_URI, MONGO_DB_NAME,
    UPSTREAM_REPO, UPSTREAM_BRANCH,
)
from helpers import (
    init_helpers,
    start_helpers,
    stop_helpers,
    get_random_helper,
    setup_helpers
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, force=True)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

app = Client(
    "MediaInfo-Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=8,
    sleep_threshold=30,
)

stream_semaphore  = asyncio.Semaphore(1)   # only 1 concurrent stream to reduce API load
channel_semaphore = asyncio.Semaphore(1)
active_users: set = set()

_last_edit:        dict[int, float] = {}
channel_queues:    dict[int, asyncio.Queue] = {}      # per-channel async queues
channel_workers:   dict[int, asyncio.Task]  = {}      # persistent drain workers
last_edit_time:    dict[int, float] = {}
_channel_edit_cnt: dict[int, int]   = defaultdict(int)
EDIT_DELAY = 10.0       # minimum seconds between edits (per channel)

# --- Flood gate (global) ---
_flood_gate: float = 0.0          # loop.time() until which streaming is blocked
_flood_gate_lock   = asyncio.Lock()
_flood_stats: dict = {"count": 0, "max_wait": 0, "last_alert": 0.0}

# --- Cooldown config ---
COOLDOWN_EVERY = 5         # inject cooldown every N channel edits
COOLDOWN_MIN   = 8.0       # seconds
COOLDOWN_MAX   = 15.0      # seconds
FLOOD_SKIP_THRESHOLD = 30  # skip streaming if gate > this many seconds

# --- Exponential backoff after floods ---
_backoff_multiplier: float = 1.0   # grows after each flood, decays on success

scheduler = AsyncIOScheduler()

# --- MongoDB collections (initialized in main()) ---
mongo_client = None
mdb = None          # database handle
mdb_queue = None    # collection: pending messages
mdb_stats = None    # collection: bot statistics


async def _init_mongo():
    """Connect to MongoDB and create indexes."""
    global mongo_client, mdb, mdb_queue, mdb_stats
    if not MONGO_URI:
        logger.warning("MONGO_URI not set — running WITHOUT persistence!")
        return
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    mdb = mongo_client[MONGO_DB_NAME]
    mdb_queue = mdb["pending_queue"]
    mdb_stats = mdb["stats"]
    await mdb_queue.create_index(
        [("chat_id", 1), ("message_id", 1)], unique=True
    )
    logger.info("MongoDB connected")


async def _db_enqueue(chat_id: int, message_id: int):
    """Persist a pending message to MongoDB."""
    if mdb_queue is None:
        return
    try:
        await mdb_queue.update_one(
            {"chat_id": chat_id, "message_id": message_id},
            {"$setOnInsert": {
                "chat_id": chat_id,
                "message_id": message_id,
                "queued_at": time.time(),
                "status": "pending",
            }},
            upsert=True,
        )
    except Exception as e:
        logger.error(f"db_enqueue error: {e}")


async def _db_dequeue(chat_id: int, message_id: int):
    """Remove a processed message from MongoDB."""
    if mdb_queue is None:
        return
    try:
        await mdb_queue.delete_one({"chat_id": chat_id, "message_id": message_id})
    except Exception as e:
        logger.error(f"db_dequeue error: {e}")


async def _db_inc_stat(field: str, value: int = 1):
    """Increment a stats counter in MongoDB."""
    if mdb_stats is None:
        return
    try:
        await mdb_stats.update_one(
            {"_id": "global"},
            {"$inc": {field: value}},
            upsert=True,
        )
    except Exception as e:
        logger.error(f"db_inc_stat error: {e}")


async def _db_get_stats() -> dict:
    """Return the current stats document."""
    if mdb_stats is None:
        return {}
    try:
        doc = await mdb_stats.find_one({"_id": "global"})
        return doc or {}
    except Exception as e:
        logger.error(f"db_get_stats error: {e}")
        return {}


async def _reload_pending_queue():
    """On startup, reload all unprocessed messages from MongoDB into
    in-memory queues so they get processed."""
    if mdb_queue is None:
        return
    reloaded = 0
    async for doc in mdb_queue.find({"status": "pending"}).sort("queued_at", 1):
        chat_id = doc["chat_id"]
        msg_id  = doc["message_id"]
        try:
            message = await app.get_messages(chat_id, msg_id)
            if not message or message.empty:
                await _db_dequeue(chat_id, msg_id)
                continue
            media = message.video or message.document
            if not media:
                await _db_dequeue(chat_id, msg_id)
                continue
            if caption_has_media_info(message.caption or ""):
                await _db_dequeue(chat_id, msg_id)
                continue
            q = _get_channel_queue(chat_id)
            await q.put(message)
            _ensure_channel_worker(chat_id)
            reloaded += 1
        except Exception as e:
            logger.warning(f"Reload skip msg {msg_id} in {chat_id}: {e}")
            await _db_dequeue(chat_id, msg_id)
    if reloaded:
        logger.info(f"Reloaded {reloaded} pending messages from MongoDB")
        await _db_inc_stat("total_reloaded", reloaded)


_LANGUAGE_MAP: dict[str, str] = {
    'en': 'English',  'eng': 'English',
    'hi': 'Hindi',    'hin': 'Hindi',
    'ta': 'Tamil',    'tam': 'Tamil',
    'te': 'Telugu',   'tel': 'Telugu',
    'ml': 'Malayalam','mal': 'Malayalam',
    'kn': 'Kannada',  'kan': 'Kannada',
    'bn': 'Bengali',  'ben': 'Bengali',
    'mr': 'Marathi',  'mar': 'Marathi',
    'gu': 'Gujarati', 'guj': 'Gujarati',
    'pa': 'Punjabi',  'pun': 'Punjabi',
    'bho':'Bhojpuri',
    'zh': 'Chinese',  'chi': 'Chinese',  'cmn': 'Chinese',
    'ko': 'Korean',   'kor': 'Korean',
    'pt': 'Portuguese','por': 'Portuguese',
    'th': 'Thai',     'tha': 'Thai',
    'tl': 'Tagalog',  'tgl': 'Tagalog',  'fil': 'Tagalog',
    'ja': 'Japanese', 'jpn': 'Japanese',
    'es': 'Spanish',  'spa': 'Spanish',
    'sv': 'Swedish',  'swe': 'Swedish',
    'fr': 'French',   'fra': 'French',   'fre': 'French',
    'de': 'German',   'deu': 'German',   'ger': 'German',
    'it': 'Italian',  'ita': 'Italian',
    'ru': 'Russian',  'rus': 'Russian',
    'ar': 'Arabic',   'ara': 'Arabic',
    'tr': 'Turkish',  'tur': 'Turkish',
    'nl': 'Dutch',    'nld': 'Dutch',    'dut': 'Dutch',
    'pl': 'Polish',   'pol': 'Polish',
    'vi': 'Vietnamese','vie': 'Vietnamese',
    'id': 'Indonesian','ind': 'Indonesian',
    'ms': 'Malay',    'msa': 'Malay',    'may': 'Malay',
    'fa': 'Persian',  'fas': 'Persian',  'per': 'Persian',
    'ur': 'Urdu',     'urd': 'Urdu',
    'he': 'Hebrew',   'heb': 'Hebrew',
    'el': 'Greek',    'ell': 'Greek',    'gre': 'Greek',
    'hu': 'Hungarian','hun': 'Hungarian',
    'cs': 'Czech',    'ces': 'Czech',    'cze': 'Czech',
    'ro': 'Romanian', 'ron': 'Romanian', 'rum': 'Romanian',
    'da': 'Danish',   'dan': 'Danish',
    'fi': 'Finnish',  'fin': 'Finnish',
    'no': 'Norwegian','nor': 'Norwegian',
    'uk': 'Ukrainian','ukr': 'Ukrainian',
    'ca': 'Catalan',  'cat': 'Catalan',
    'hr': 'Croatian', 'hrv': 'Croatian',
    'sk': 'Slovak',   'slk': 'Slovak',   'slo': 'Slovak',
    'sr': 'Serbian',  'srp': 'Serbian',
    'bg': 'Bulgarian','bul': 'Bulgarian',
    'unknown': 'Original Audio',
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    if not code:
        return 'Unknown'
    cleaned = code.split('(')[0].strip()
    return _LANGUAGE_MAP.get(cleaned.lower(), 'Original Audio')


@lru_cache(maxsize=64)
def get_standard_resolution(height: int) -> Optional[str]:
    if not height:
        return None
    if height <= 240:  return "240p"
    if height <= 360:  return "360p"
    if height <= 480:  return "480p"
    if height <= 720:  return "720p"
    if height <= 1080: return "1080p"
    if height <= 1440: return "1440p"
    if height <= 2160: return "2160p"
    return "2160p+"


@lru_cache(maxsize=128)
def get_video_format(codec: str, transfer: str = '', hdr: str = '', bit_depth: str = '') -> Optional[str]:
    if not codec:
        return None
    codec = codec.lower()
    parts: list[str] = []

    if   any(x in codec for x in ('hevc', 'h.265', 'h265')):  parts.append('HEVC')
    elif 'av1' in codec:                                        parts.append('AV1')
    elif any(x in codec for x in ('avc', 'avc1', 'h.264', 'h264')): parts.append('x264')
    elif 'vp9' in codec:                                        parts.append('VP9')
    elif any(x in codec for x in ('mpeg4', 'xvid')):            parts.append('MPEG4')
    else:
        return None

    try:
        if bit_depth and int(bit_depth) > 8:
            parts.append(f"{bit_depth}bit")
    except (ValueError, TypeError):
        pass

    t = transfer.lower();  h = hdr.lower()
    if any(x in t for x in ('pq', 'hlg', 'smpte', '2084', 'st 2084')) or 'hdr' in h or 'dolby' in h:
        parts.append('HDR')

    return ' '.join(parts)


def _is_video_track(track: dict) -> bool:
    t      = (track.get('@type',        '') or '').lower()
    fmt    = (track.get('Format',       '') or '').lower()
    cid    = (track.get('CodecID',      '') or '').lower()
    fp     = (track.get('Format_Profile','') or '').lower()
    title  = (track.get('Title',        '') or '').lower()
    menu   = str(track.get('MenuID',    '') or '').lower()

    return any([
        t == 'video',
        any(x in fmt for x in ('avc','hevc','h.264','h264','h.265','h265','av1','vp9','mpeg-4','mpeg4','xvid')),
        any(x in cid for x in ('avc','h264','hevc','h265','av1','vp9','mpeg4','xvid','27')),
        'video' in menu,
        'video' in title,
        any(x in fp  for x in ('main','high','baseline')),
    ])


def _has_subtitles(tracks: list) -> bool:
    for track in tracks:
        if not isinstance(track, dict):
            continue
        t   = (track.get('@type',       '') or '').lower()
        fmt = (track.get('Format',      '') or '').lower()
        cid = (track.get('CodecID',     '') or '').lower()
        enc = (track.get('Encoding',    '') or '').lower()
        fi  = (track.get('Format_Info', '') or '').lower()
        ttl = (track.get('Title',       '') or '').lower()
        if any([
            t == 'text',
            any(x in fmt for x in ('pgs','subrip','ass','ssa','srt','dvb_subtitle','dvd_subtitle')),
            any(x in cid for x in ('s_text','subp','pgs','subtitle','dvb','dvd')),
            any(x in enc for x in ('utf-8','utf8','unicode','text')),
            any(x in fi  for x in ('subtitle','caption','text')),
            'subtitle' in ttl,
        ]):
            return True
    return False


def _parse_int(value) -> int:
    try:
        return int(re.findall(r"\d+", str(value))[0])
    except Exception:
        return 0


def _parse_duration(value) -> float:
    try:
        if not value:
            return 0
        v = str(value).strip()
        if v.replace('.', '', 1).lstrip('-').isdigit():
            f = float(v)
            if f > 86_400_000:
                return f / 1_000_000
            if f > 86_400:
                return f / 1_000
            return f
        if ':' in v:
            parts = [float(p) for p in v.split(':')]
            if len(parts) == 3:
                return parts[0]*3600 + parts[1]*60 + parts[2]
            if len(parts) == 2:
                return parts[0]*60 + parts[1]
    except Exception:
        pass
    return 0


def _fmt_duration(s: float) -> str:
    s = int(s)
    return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"


async def _run_mediainfo(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_shell(
            f'mediainfo --ParseSpeed=0 --Language=raw --Output=JSON "{path}"',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        except asyncio.TimeoutError:
            proc.kill();  await proc.wait()
            return {}
        return json.loads(stdout.decode() or '{}')
    except Exception as e:
        logger.warning(f"mediainfo error: {e}")
        return {}


async def _run_ffprobe_full(path: str) -> dict:
    try:
        proc = await asyncio.create_subprocess_shell(
            f'ffprobe -v error -show_streams -show_format -of json "{path}"',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=8)
        return json.loads(out.decode() or '{}')
    except Exception as e:
        logger.warning(f"ffprobe error: {e}")
        return {}


def _parse_ffprobe(data: dict) -> tuple:
    streams  = data.get('streams', [])
    fmt      = data.get('format',  {})

    duration  = 0.0
    width = height = None
    codec = bit_depth = hdr = transfer = ''
    audio_langs: set[str] = set()
    sub_langs:   set[str] = set()
    has_sub = False

    dur_raw = fmt.get('duration') or ''
    if dur_raw:
        duration = _parse_duration(dur_raw)

    for s in streams:
        ctype = (s.get('codec_type') or '').lower()
        tags  = s.get('tags') or {}

        if ctype == 'video':
            if not width:
                width  = s.get('width')  or s.get('coded_width')
            if not height:
                height = s.get('height') or s.get('coded_height')

            codec_raw = (s.get('codec_name') or '').lower()
            if   'hevc' in codec_raw or 'h265' in codec_raw: codec = 'hevc'
            elif 'h264' in codec_raw or 'avc'  in codec_raw: codec = 'avc'
            elif 'av1'  in codec_raw:                          codec = 'av1'
            elif 'vp9'  in codec_raw:                          codec = 'vp9'
            elif 'mpeg4' in codec_raw or 'xvid' in codec_raw: codec = 'mpeg4'
            else: codec = codec_raw

            bps = str(s.get('bits_per_raw_sample') or s.get('bits_per_coded_sample') or '')
            if bps.isdigit() and bps != '0':
                bit_depth = bps

            ct = (s.get('color_transfer') or '').lower()
            cs = (s.get('color_space')    or '').lower()
            if any(x in ct for x in ('smpte2084', 'arib-std-b67', 'smpte428')):
                hdr = 'HDR'
            elif 'bt2020' in cs and not hdr:
                hdr = 'HDR'

            if not duration:
                d = tags.get('DURATION') or tags.get('duration') or ''
                if d:
                    duration = _parse_duration(d)

        elif ctype == 'audio':
            lang = tags.get('language') or tags.get('LANGUAGE') or ''
            audio_langs.add(get_full_language_name(lang or 'unknown'))

        elif ctype == 'subtitle':
            has_sub = True
            lang = tags.get('language') or tags.get('LANGUAGE') or ''
            if lang:
                sub_langs.add(get_full_language_name(lang))

    audio_str = ', '.join(sorted(audio_langs)) if audio_langs else 'Original Audio'
    if sub_langs:
        sub_str = ', '.join(sorted(sub_langs))
    elif has_sub:
        sub_str = 'ESUB'
    else:
        sub_str = 'No Esubs'

    return duration, width, height, codec, bit_depth, hdr, transfer, audio_str, sub_str


def _parse_tracks(tracks: list) -> tuple:
    duration  = 0.0
    width = height = None
    codec = bit_depth = hdr = transfer = ''
    audio_langs: set[str] = set()
    sub_langs:   set[str] = set()

    for track in tracks:
        if not isinstance(track, dict):
            continue
        t = (track.get('@type', '') or '').lower()

        if t == 'general':
            if not duration:
                duration = _parse_duration(track.get('Duration'))

        elif _is_video_track(track):
            for field in ('Height', 'Sampled_Height', 'Encoded_Height'):
                raw = str(track.get(field, '') or '').split()[0]
                if raw.isdigit():
                    height = int(raw)
                    break

            for field in ('Width', 'Sampled_Width', 'Encoded_Width'):
                raw = str(track.get(field, '') or '').split()[0]
                if raw.isdigit():
                    width = int(raw)
                    break

            codec     = (track.get('Format', '') or '').lower()
            bit_depth = track.get('BitDepth', '') or ''
            transfer  = (track.get('transfer_characteristics', '') or
                         track.get('TransferCharacteristics', '') or '').lower()
            hdr       = (track.get('HDR_Format', '') or
                         track.get('HDR_Format_Compatibility', '') or '')

            if not duration:
                duration = _parse_duration(track.get('Duration'))

            track_str = str(track).lower()
            if 'dolby vision' in track_str:
                hdr = 'Dolby Vision'
            elif 'hdr' in track_str and not hdr:
                hdr = 'HDR'

        elif t == 'audio':
            lang = None
            for field in ('Language', 'Language_String', 'Title'):
                v = track.get(field)
                if v:
                    lang = v
                    break
            audio_langs.add(get_full_language_name(lang or 'unknown'))

        elif t in ('text', 'menu', 'subtitle'):
            lang = track.get('Language') or track.get('Language_String') or 'unknown'
            sub_langs.add(get_full_language_name(lang))

    audio_str = ', '.join(sorted(audio_langs)) if audio_langs else 'Original Audio'
    sub_str   = ', '.join(sorted(sub_langs))   if sub_langs   else (
                    'ESUB' if _has_subtitles(tracks) else 'No Esubs')

    return duration, width, height, codec, bit_depth, hdr, transfer, audio_str, sub_str


async def _probe(path: str) -> tuple:
    mi_data = await _run_mediainfo(path)
    tracks  = mi_data.get('media', {}).get('track', [])
    mi      = _parse_tracks(tracks)
    mi_dur, mi_w, mi_h = mi[0], mi[1], mi[2]

    fp_data = await _run_ffprobe_full(path)
    fp      = _parse_ffprobe(fp_data) if fp_data else None

    if fp is None:
        return mi

    fp_dur, fp_w, fp_h = fp[0], fp[1], fp[2]

    duration  = mi_dur or fp_dur
    width     = mi_w   or fp_w
    height    = mi_h   or fp_h
    codec     = mi[3]  or fp[3]
    bit_depth = mi[4]  or fp[4]
    hdr       = mi[5]  or fp[5]
    transfer  = mi[6]  or fp[6]
    audio     = mi[7] if mi[7] != 'Unknown' else fp[7]
    subtitle  = mi[8] if mi[8] != 'No Sub'  else fp[8]

    return duration, width, height, codec, bit_depth, hdr, transfer, audio, subtitle


def _build_caption(message, media, result: tuple) -> str:
    duration, width, height, codec, bit_depth, hdr, transfer, audio, sub = result

    quality     = get_standard_resolution(min(w for w in (width, height) if w) if width and height else (height or width or 0))
    fmt         = get_video_format(codec, transfer, hdr, bit_depth)
    video_line  = ' '.join(filter(None, [quality, fmt])) or 'Unknown'

    return CAPTION_TEMPLATE.format(
        title     = message.caption or getattr(media, 'file_name', None) or 'Video',
        video_line= video_line,
        duration  = _fmt_duration(duration) if duration else 'Unknown',
        audio     = audio,
        subtitle  = sub,
    )


def caption_has_media_info(caption: str) -> bool:
    if not caption:
        return False
    hits = (
        bool(re.search(r'🎬', caption)),
        bool(re.search(r'⏳\s*\d{2}:\d{2}:\d{2}', caption)),
        bool(re.search(r'🔊', caption)),
        bool(re.search(r'💬', caption)),
    )
    return sum(hits) >= 2


_STREAM_STEPS = [
    ("3MB",   3   * 1024 * 1024),
]


async def _stream_chunk(media, size: int, path: str) -> bool:
    global _flood_gate
    loop = asyncio.get_event_loop()

    # Respect global flood gate
    gate_remaining = _flood_gate - loop.time()
    if gate_remaining > 0:
        if gate_remaining > FLOOD_SKIP_THRESHOLD:
            logger.warning(f"Flood gate active {gate_remaining:.0f}s — skipping stream")
            return False
        logger.info(f"Flood gate: sleeping {gate_remaining:.1f}s")
        await asyncio.sleep(gate_remaining)

    async def _do_stream() -> bool:
        written = 0
        async with stream_semaphore:
            async with aiopen(path, 'wb') as f:
                async for chunk in app.stream_media(media):
                    if not chunk:
                        break
                    remaining = size - written
                    if remaining <= 0:
                        break
                    piece = chunk[:remaining]
                    await f.write(piece)
                    written += len(piece)
                    if written >= size:
                        break
        return os.path.exists(path) and os.path.getsize(path) > 0

    try:
        return await _do_stream()
    except FloodWait as e:
        wait = e.value
        async with _flood_gate_lock:
            _flood_gate = loop.time() + wait
            _flood_stats["count"] += 1
            _flood_stats["max_wait"] = max(_flood_stats["max_wait"], wait)
        logger.warning(f"FloodWait {wait}s on stream_chunk (size={size})")
        if wait > 60:
            # Alert admin for big floods (at most once per 5 min)
            if loop.time() - _flood_stats["last_alert"] > 300:
                _flood_stats["last_alert"] = loop.time()
                asyncio.create_task(_notify_admin(
                    f"⚠️ <b>FloodWait Alert</b>\n"
                    f"Wait: <code>{wait}s</code>\n"
                    f"Total floods: <code>{_flood_stats['count']}</code>\n"
                    f"Max ever: <code>{_flood_stats['max_wait']}s</code>"
                ))
            return False  # Don't wait 60+ seconds inline
        await asyncio.sleep(wait + random.uniform(1, 3))
        try:
            return await _do_stream()
        except Exception as retry_err:
            logger.warning(f"stream_chunk retry failed ({size}): {retry_err}")
            return False
    except Exception as e:
        logger.warning(f"stream_chunk failed ({size}): {e}")
        return False


async def process_message(message, progress_msg=None) -> tuple[str, Optional[str]]:
    global _flood_gate, _backoff_multiplier
    media = message.video or message.document
    loop  = asyncio.get_event_loop()

    async def _update(text: str):
        if progress_msg:
            await _safe_edit(progress_msg, text)
            await asyncio.sleep(0.3)

    gate_remaining = _flood_gate - loop.time()
    
    # 1. Metadata fast-path if flood gate is highly active (skip streaming entirely)
    if gate_remaining > FLOOD_SKIP_THRESHOLD or _backoff_multiplier > 2.0:
        logger.info(f"Flood restrictions active, using fast metadata for {message.id}")
        v = message.video
        if v:
            result = (v.duration or 0, v.width or 0, v.height or 0, '', '', '', '', 'Unknown', 'Unknown')
            return _build_caption(message, media, result), None

    await _update("⚡ Fast scan (3MB)…")

    for label, size in _STREAM_STEPS:
        # Abort streaming loop if flood gate is too long
        gate_remaining = _flood_gate - loop.time()
        if gate_remaining > FLOOD_SKIP_THRESHOLD:
            logger.info(f"Skipping '{label}' — flood gate active {gate_remaining:.0f}s")
            break

        tmp = f"probe_{label}_{message.id}_{uuid.uuid4().hex[:8]}.bin"
        try:
            await _update(f"📦 Scanning {label}…")
            ok = await _stream_chunk(media, size, tmp)
            if not ok:
                continue

            result = await _probe(tmp)
            _, w, h = result[0], result[1], result[2]
            if w or h:
                return _build_caption(message, media, result), None

        except Exception as e:
            logger.warning(f"{label} probe error: {e}")
        finally:
            if os.path.exists(tmp):
                await aioremove(tmp)

    await _update("⬇️ Full download (fallback)…")
    try:
        file_size = getattr(media, 'file_size', 0) or 0
        if file_size > 2 * 1024 ** 3:
            return message.caption or getattr(media, 'file_name', None) or 'Video', None

        for attempt in range(3):
            try:
                file_path = await asyncio.wait_for(message.download(), timeout=90)
                break
            except FloodWait as e:
                wait = e.value
                logger.warning(f"FloodWait {wait}s on download (attempt {attempt+1})")
                async with _flood_gate_lock:
                    _flood_gate = loop.time() + wait
                    _flood_stats["count"] += 1
                    _flood_stats["max_wait"] = max(_flood_stats["max_wait"], wait)
                if wait > 60:
                    return message.caption or getattr(media, 'file_name', None) or 'Video', None
                await asyncio.sleep(wait + random.uniform(1, 3))
            except asyncio.TimeoutError:
                logger.error("Download timed out")
                return message.caption or getattr(media, 'file_name', None) or 'Video', None
        else:
            return message.caption or getattr(media, 'file_name', None) or 'Video', None

        result = await _probe(file_path)
        return _build_caption(message, media, result), file_path

    except Exception as e:
        logger.error(f"Full download failed: {e}")

    return message.caption or getattr(media, 'file_name', None) or 'Video', None


async def _safe_edit(msg, text: str, parse_mode=None):
    if not msg:
        return
    key  = msg.id
    now  = asyncio.get_event_loop().time()
    if key in _last_edit and now - _last_edit[key] < 1.7:
        return
    try:
        await msg.edit_text(text, parse_mode=parse_mode)
        _last_edit[key] = now
    except (MessageNotModified, Exception):
        pass


def _get_channel_queue(channel_id: int) -> asyncio.Queue:
    """Return (and lazily create) the async queue for a channel."""
    if channel_id not in channel_queues:
        channel_queues[channel_id] = asyncio.Queue()
    return channel_queues[channel_id]


def _ensure_channel_worker(channel_id: int):
    """Ensure a persistent background worker exists for this channel."""
    task = channel_workers.get(channel_id)
    if task is None or task.done():
        channel_workers[channel_id] = asyncio.create_task(
            _channel_worker(channel_id)
        )


async def _channel_worker(channel_id: int):
    """Persistent worker that drains the channel queue forever.

    Each item in the queue is a raw Pyrogram message.
    The worker does:
      1. probe/process the message
      2. respect edit-delay & periodic cooldown
      3. edit the caption on Telegram
    Messages that arrive while the worker is sleeping (cooldown / flood)
    simply accumulate in the asyncio.Queue and are processed in order.
    """
    global EDIT_DELAY, _backoff_multiplier
    loop = asyncio.get_event_loop()
    q    = _get_channel_queue(channel_id)

    while True:
        # Block until a message is available (no busy-wait)
        message = await q.get()
        file_path = None
        try:
            # --- 1. Process (probe) the message ---
            caption, file_path = await process_message(message)

            # --- 2. Enforce minimum edit delay with exponential backoff ---
            now  = loop.time()
            last = last_edit_time.get(channel_id, 0)
            
            current_delay = EDIT_DELAY * _backoff_multiplier
            
            if now - last < current_delay:
                await asyncio.sleep(current_delay - (now - last))

            # --- 3. Periodic cooldown ---
            _channel_edit_cnt[channel_id] += 1
            if _channel_edit_cnt[channel_id] % COOLDOWN_EVERY == 0:
                cooldown = random.uniform(COOLDOWN_MIN, COOLDOWN_MAX) * _backoff_multiplier
                logger.info(f"Channel {channel_id}: cooldown {cooldown:.1f}s "
                            f"({q.qsize()} queued)")
                await asyncio.sleep(cooldown)

            # --- 4. Edit caption on Telegram ---
            edited = False
            try:
                helper = get_random_helper()
                client_to_use = helper if helper else app
                await client_to_use.edit_message_caption(
                    chat_id=channel_id,
                    message_id=message.id,
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
                last_edit_time[channel_id] = loop.time()
                edited = True
            except FloodWait as e:
                wait = e.value
                EDIT_DELAY = max(EDIT_DELAY, wait / 10 + 2)
                _backoff_multiplier = min(10.0, _backoff_multiplier * 1.5)
                logger.warning(f"FloodWait {wait}s on edit_caption ch={channel_id} "
                               f"({q.qsize()} queued) | backoff={_backoff_multiplier:.1f}x")
                await asyncio.sleep(wait + random.uniform(1, 3))
                try:
                    helper = get_random_helper()
                    client_to_use = helper if helper else app
                    await client_to_use.edit_message_caption(
                        chat_id=channel_id,
                        message_id=message.id,
                        caption=caption,
                        parse_mode=ParseMode.HTML
                    )
                    last_edit_time[channel_id] = loop.time()
                    edited = True
                except Exception as err:
                    logger.error(f"Retry edit failed: {err}")
            except Exception as e:
                logger.error(f"Edit failed: {e}")

            # --- 5. Remove from DB only after successful edit ---
            if edited:
                _backoff_multiplier = max(1.0, _backoff_multiplier * 0.95) # gradual decay
                await _db_dequeue(channel_id, message.id)
                await _db_inc_stat("total_processed")

        except Exception as exc:
            logger.error(f"Channel worker error ch={channel_id}: {exc}")
        finally:
            if file_path and os.path.exists(file_path):
                await aioremove(file_path)
            q.task_done()


@app.on_message(
    filters.chat(ALLOWED_CHATS) & filters.channel &
    (filters.video | filters.document)
)
async def channel_handler(_, message):
    if caption_has_media_info(message.caption or ''):
        return

    channel_id = message.chat.id

    # Persist to MongoDB FIRST (survives cooldown, flood, restart)
    await _db_enqueue(channel_id, message.id)
    await _db_inc_stat("total_received")

    q = _get_channel_queue(channel_id)
    await q.put(message)            # queue the RAW message immediately
    _ensure_channel_worker(channel_id)
    logger.info(f"Queued msg {message.id} for ch={channel_id} "
                f"(queue size: {q.qsize()})")


@app.on_message(filters.private & (filters.video | filters.document))
async def private_handler(_, message):
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply_text("⚠️ Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message):
    file_path = None
    progress_msg = None
    user_id = message.from_user.id
    try:
        await asyncio.sleep(0.5)
        progress_msg = await message.reply_text("⏳ Processing…")
        caption, file_path = await process_message(message, progress_msg)
        try:
            await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML)
        except MessageNotModified:
            pass
    except Exception as e:
        logger.error(f"Private handler error: {e}")
    finally:
        active_users.discard(user_id)
        if file_path and os.path.exists(file_path):
            await aioremove(file_path)


@app.on_message(filters.command("info") & filters.reply)
async def info_command(_, message):
    reply = message.reply_to_message
    if not (reply and (reply.video or reply.document)):
        return await message.reply_text("⚠️ Reply to a video or document.")

    media = reply.video or reply.document
    tmp   = f"info_{reply.id}_{uuid.uuid4().hex[:6]}.bin"
    try:
        ok = await _stream_chunk(media, 8 * 1024 * 1024, tmp)
        if not ok:
            tmp2 = await reply.download()
            result = await _probe(tmp2)
            os.remove(tmp2)
        else:
            result = await _probe(tmp)

        caption = _build_caption(reply, media, result)
        await message.reply_text(caption, parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"❌ Failed\n\n<code>{e}</code>", parse_mode=ParseMode.HTML)
    finally:
        if os.path.exists(tmp):
            await aioremove(tmp)


@app.on_message(filters.command("start") & filters.private)
async def start(_, m):
    await m.reply_text(
        "<b>🎬 Media Info Bot</b>\n\n"
        "Send me any video or file and I'll extract detailed media information.\n\n"
        "I provide:\n"
        "• 🎞 Video quality, codec &amp; bit depth\n"
        "• ⏳ Duration\n"
        "• 🔊 Audio languages\n"
        "• 💬 Subtitle info\n\n"
        "<b>⚡ Fast • Clean • Accurate</b>\n\n"
        "📌 <i>Note:</i> Send one file at a time.",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("server") & filters.user(ADMIN_ID))
async def server_cmd(_, m):
    await m.reply_text(
        f"CPU: {psutil.cpu_percent()}%\n"
        f"RAM: {psutil.virtual_memory().percent}%\n"
        f"Disk: {psutil.disk_usage('/').percent}%"
    )


@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(_, m):
    db_stats = await _db_get_stats()
    total_received  = db_stats.get("total_received", 0)
    total_processed = db_stats.get("total_processed", 0)
    total_reloaded  = db_stats.get("total_reloaded", 0)

    total_queued = sum(q.qsize() for q in channel_queues.values())
    db_pending = 0
    if mdb_queue is not None:
        try:
            db_pending = await mdb_queue.count_documents({"status": "pending"})
        except Exception:
            pass

    text = (
        f"📊 <b>Bot Statistics</b>\n\n"
        f"📥 Total received: <code>{total_received}</code>\n"
        f"✅ Total processed: <code>{total_processed}</code>\n"
        f"📬 In-memory queue: <code>{total_queued}</code>\n"
        f"💾 DB pending: <code>{db_pending}</code>\n"
        f"🔄 Reloaded on restart: <code>{total_reloaded}</code>\n\n"
        f"🌊 Flood events: <code>{_flood_stats['count']}</code>\n"
        f"⏱ Max wait seen: <code>{_flood_stats['max_wait']}s</code>"
    )
    await m.reply_text(text, parse_mode=ParseMode.HTML)


@app.on_message(filters.command("setup") & filters.user(ADMIN_ID))
async def setup_cmd(_, m):
    # Determine target chat ID
    if m.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
        target_chat = m.chat.id
    elif len(m.command) > 1:
        try:
            target_chat = int(m.command[1])
        except ValueError:
            target_chat = m.command[1]
    else:
        await m.reply_text("Please run this command in the channel/group or pass a chat ID: `/setup -100...`")
        return

    msg = await m.reply_text("🔄 Adding helper bots as admins...")
    
    total, success, failed = await setup_helpers(app, target_chat)
    
    owner_linked = "Unknown"
    try:
        chat = await app.get_chat(target_chat)
        owner_linked = getattr(chat, 'linked_chat', None)
        owner_linked = owner_linked.id if owner_linked else "None"
    except Exception:
        pass

    text = f"✅ Setup complete!\n\n" \
           f"📢 Channel: <code>{target_chat}</code>\n" \
           f"👑 Owner Linked: <code>{owner_linked}</code>\n" \
           f"🤖 Added: {success}/{total}\n" \
           f"🤖 Now Start Sending Files In This Channel To Get Resumable And Fast Download Links.\n"

    if failed:
        text += f"⚠️ Failed: {', '.join(failed)}"

    await msg.edit_text(text)


@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, m):
    if UPSTREAM_REPO:
        status = await m.reply_text("🔄 Pulling from upstream…")
        try:
            # Ensure upstream remote is set correctly
            proc = await asyncio.create_subprocess_shell(
                f'git remote set-url upstream {UPSTREAM_REPO} 2>/dev/null || '
                f'git remote add upstream {UPSTREAM_REPO}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await proc.communicate()

            # Force-pull from upstream (overwrite local changes)
            proc = await asyncio.create_subprocess_shell(
                f'git fetch upstream {UPSTREAM_BRANCH} && '
                f'git reset --hard upstream/{UPSTREAM_BRANCH}',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
            out = (stdout.decode() + stderr.decode()).strip()

            if proc.returncode != 0:
                await status.edit_text(
                    f"⚠️ Git pull failed:\n<code>{out[-500:]}</code>\n\nRestarting anyway…",
                    parse_mode=ParseMode.HTML,
                )
            else:
                # Install any new/updated dependencies
                pip_proc = await asyncio.create_subprocess_shell(
                    'pip install -r requirements.txt --no-cache-dir -q',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(pip_proc.communicate(), timeout=120)
                await status.edit_text(
                    f"✅ Updated from upstream\n<code>{out[-300:]}</code>\n\nRestarting…",
                    parse_mode=ParseMode.HTML,
                )
        except asyncio.TimeoutError:
            await status.edit_text("⚠️ Git pull timed out. Restarting anyway…")
        except Exception as e:
            await status.edit_text(f"⚠️ Update error: {e}\nRestarting anyway…")
    else:
        await m.reply_text("Restarting…")
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("shutdown") & filters.user(ADMIN_ID))
async def shutdown_cmd(_, m):
    await m.reply_text("Shutting down…")
    scheduler.shutdown(wait=False)
    await stop_helpers()
    await app.stop()
    os._exit(0)


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_cmd(_, m):
    await m.reply_text("Updating…")
    try:
        os.system("git pull")
        os.system("pip install -r requirements.txt --no-cache-dir -q")
        await m.reply_text("✅ Updated. Restarting…")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await m.reply_text(f"Update failed: {e}")


async def _notify_admin(text: str):
    """Send an alert message to ADMIN_ID, silently ignore errors."""
    try:
        await app.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Admin notify failed: {e}")


async def _send_status_report():
    """Scheduled job: send bot health + flood stats to admin."""
    try:
        total_queued = sum(q.qsize() for q in channel_queues.values())
        gate_remaining = max(0.0, _flood_gate - asyncio.get_event_loop().time())

        # Get DB stats
        db_stats = await _db_get_stats()
        total_received  = db_stats.get("total_received", 0)
        total_processed = db_stats.get("total_processed", 0)
        db_pending = 0
        if mdb_queue is not None:
            try:
                db_pending = await mdb_queue.count_documents({"status": "pending"})
            except Exception:
                pass

        text = (
            f"📊 <b>Bot Status Report</b>\n\n"
            f"🖥 CPU: <code>{psutil.cpu_percent()}%</code>\n"
            f"💾 RAM: <code>{psutil.virtual_memory().percent}%</code>\n"
            f"💿 Disk: <code>{psutil.disk_usage('/').percent}%</code>\n\n"
            f"📥 Total received: <code>{total_received}</code>\n"
            f"✅ Total processed: <code>{total_processed}</code>\n"
            f"📬 In-memory queue: <code>{total_queued}</code>\n"
            f"💾 DB pending: <code>{db_pending}</code>\n"
            f"👥 Active users: <code>{len(active_users)}</code>\n\n"
            f"🌊 Flood events: <code>{_flood_stats['count']}</code>\n"
            f"⏱ Max wait seen: <code>{_flood_stats['max_wait']}s</code>\n"
            f"🚧 Gate active: <code>{gate_remaining:.0f}s</code>"
        )
        await app.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Status report failed: {e}")


def _install_deps():
    for binary, pkg in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo")):
        r = subprocess.run(["which", binary], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if r.returncode != 0:
            logger.info(f"Installing {pkg}…")
            subprocess.run(["apt", "update", "-y"], stdout=subprocess.DEVNULL)
            subprocess.run(["apt", "install", "-y", pkg], stdout=subprocess.DEVNULL)


async def main():
    gc.set_threshold(*GC_THRESHOLD)
    _install_deps()

    # Initialize MongoDB (must happen before app.start so reload works)
    await _init_mongo()

    init_helpers()

    await app.start()
    await start_helpers()

    me = await app.get_me()
    logger.info(f"@{me.username} started")

    # Reload any messages that were pending from previous session
    await _reload_pending_queue()

    db_stats = await _db_get_stats()
    db_pending = 0
    if mdb_queue is not None:
        try:
            db_pending = await mdb_queue.count_documents({"status": "pending"})
        except Exception:
            pass
    startup_msg = (
        f"🚀 Bot Started\n\n"
        f"📥 Total received (all time): <code>{db_stats.get('total_received', 0)}</code>\n"
        f"✅ Total processed (all time): <code>{db_stats.get('total_processed', 0)}</code>\n"
        f"💾 Pending from DB: <code>{db_pending}</code>"
    )
    await app.send_message(ADMIN_ID, startup_msg, parse_mode=ParseMode.HTML)

    scheduler.add_job(gc.collect, "interval", minutes=20)
    scheduler.add_job(_send_status_report, "interval", minutes=30)
    scheduler.start()

    await asyncio.Event().wait()


if __name__ == "__main__":
    app.run(main())
