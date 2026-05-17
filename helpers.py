import random
import logging
import asyncio
from pyrogram import Client
from pyrogram.types import ChatPrivileges
from pyrogram.errors import RPCError, ChatAdminRequired, UserAdminInvalid, FloodWait
from config import API_ID, API_HASH, HELPER_TOKENS

logger = logging.getLogger(__name__)

helper_clients: list[Client] = []
_started_helpers: list[Client] = []
helper_info: dict = {}  # client -> User object

def init_helpers():
    """Initialize Pyrogram Client instances for all helper tokens."""
    for i, token in enumerate(HELPER_TOKENS, start=1):
        try:
            client = Client(
                f"helper_{i}",
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=token,
                in_memory=True
            )
            helper_clients.append(client)
        except Exception as e:
            logger.error(f"Failed to init helper {i}: {e}")

async def start_helpers():
    """Start all helper clients and fetch their bot info."""
    for client in helper_clients:
        try:
            await client.start()
            _started_helpers.append(client)
            me = await client.get_me()
            helper_info[client] = me
        except Exception as e:
            logger.error(f"Failed to start helper: {e}")
    logger.info(f"Started {len(_started_helpers)} helper bots.")

async def stop_helpers():
    """Stop all helper clients."""
    for client in _started_helpers:
        try:
            await client.stop()
        except:
            pass

def get_random_helper() -> Client | None:
    """Return a random active helper client, or None if none available."""
    if not _started_helpers:
        return None
    return random.choice(_started_helpers)

async def setup_helpers(app: Client, chat_id: int) -> tuple[int, int, list[str]]:
    """
    Attempt to add all helper bots to the given chat as admins.
    Returns: (total_helpers, successfully_added, list_of_failed_usernames)
    """
    if not _started_helpers:
        return 0, 0, []

    privileges = ChatPrivileges(
        can_manage_chat=True,
        can_edit_messages=True,
        can_delete_messages=True,
        can_post_messages=True,
    )

    success_count = 0
    failed_bots = []

    for client in _started_helpers:
        bot = helper_info.get(client)
        if not bot:
            continue
        try:
            await app.promote_chat_member(
                chat_id=chat_id,
                user_id=bot.username or bot.id,
                privileges=privileges
            )
            success_count += 1
            await asyncio.sleep(0.5)  # Avoid hitting limits while adding
        except FloodWait as e:
            logger.warning(f"FloodWait while adding helper {bot.username}: {e}")
            failed_bots.append(f"@{bot.username}")
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            logger.error(f"Could not add helper {bot.username} to {chat_id}: {e}")
            failed_bots.append(f"@{bot.username}")

    return len(_started_helpers), success_count, failed_bots
