from __future__ import annotations

import logging

from telethon import TelegramClient
from telethon.errors import (
    ChatWriteForbiddenError,
    PeerFloodError,
    UserPrivacyRestrictedError,
)

from ..utils.retry import retry_on_flood

logger = logging.getLogger(__name__)


class SendResult:
    def __init__(
        self,
        thread_msg_id: int | None = None,
        dm_msg_id: int | None = None,
        peer_flood: bool = False,
    ) -> None:
        self.thread_msg_id = thread_msg_id
        self.dm_msg_id = dm_msg_id
        self.peer_flood = peer_flood


class Sender:
    def __init__(self, client: TelegramClient) -> None:
        self._client = client

    @retry_on_flood(max_retries=3)
    async def send_thread_reply(
        self, chat_id: int, reply_to_msg_id: int, text: str
    ) -> int | None:
        try:
            msg = await self._client.send_message(
                chat_id, text, reply_to=reply_to_msg_id
            )
            logger.info("Sent thread reply in chat %d (msg_id=%d)", chat_id, msg.id)
            return msg.id
        except ChatWriteForbiddenError:
            logger.warning("Cannot write to chat %d — forbidden", chat_id)
            return None

    @retry_on_flood(max_retries=3)
    async def send_dm(self, user_id: int, text: str) -> int | None:
        try:
            msg = await self._client.send_message(user_id, text)
            logger.info("Sent DM to user %d (msg_id=%d)", user_id, msg.id)
            return msg.id
        except UserPrivacyRestrictedError:
            logger.warning("User %d has privacy restrictions — skipping DM", user_id)
            return None

    async def send_outreach(
        self,
        chat_id: int,
        reply_to_msg_id: int,
        thread_text: str,
        user_id: int | None,
        dm_text: str | None,
    ) -> SendResult:
        try:
            thread_id = await self.send_thread_reply(chat_id, reply_to_msg_id, thread_text)
            dm_id = None
            if user_id and dm_text:
                dm_id = await self.send_dm(user_id, dm_text)
            return SendResult(thread_msg_id=thread_id, dm_msg_id=dm_id)
        except PeerFloodError:
            logger.error("PeerFloodError — outreach must stop for 24h")
            return SendResult(peer_flood=True)
