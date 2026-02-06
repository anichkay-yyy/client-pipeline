from __future__ import annotations

import logging

from telethon import TelegramClient

logger = logging.getLogger(__name__)


def create_userbot(
    session_name: str,
    api_id: int,
    api_hash: str,
) -> TelegramClient:
    return TelegramClient(session_name, api_id, api_hash)


async def start_userbot(
    client: TelegramClient,
    phone: str,
) -> TelegramClient:
    await client.start(phone=phone)  # type: ignore[arg-type]
    me = await client.get_me()
    logger.info("Userbot started as %s (id=%s)", me.username, me.id)  # type: ignore[union-attr]
    return client
