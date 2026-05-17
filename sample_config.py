import os

API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))

_raw = os.environ.get("ALLOWED_CHATS", "")
ALLOWED_CHATS = [int(x.strip()) for x in _raw.split(",") if x.strip()]

LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "[%(asctime)s][%(name)s][%(module)s][%(lineno)d][%(levelname)s] -> %(message)s",
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

GC_THRESHOLD = (
    int(os.environ.get("GC_THRESHOLD_0", 500)),
    int(os.environ.get("GC_THRESHOLD_1", 5)),
    int(os.environ.get("GC_THRESHOLD_2", 5)),
)

CAPTION_TEMPLATE = os.environ.get(
    "CAPTION_TEMPLATE",
    "<b>{title}</b>\n\n"
    "🎬 <b>{video_line}</b> | ⏳ <b>{duration}</b>\n"
    "🔊 <b>{audio}</b>\n"
    "💬 <b>{subtitle}</b>\n\n"
)

# --- MongoDB ---
MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "mediainfo_bot")

# --- Upstream ---
UPSTREAM_REPO = os.environ.get("UPSTREAM_REPO", "")      # e.g. https://github.com/user/MediaInfo-Bot
UPSTREAM_BRANCH = os.environ.get("UPSTREAM_BRANCH", "main")

# --- Helper Bots ---
_helper_tokens_raw = os.environ.get("HELPER_TOKENS", "")
HELPER_TOKENS = [x.strip() for x in _helper_tokens_raw.split() if x.strip()]
