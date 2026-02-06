from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from telethon import TelegramClient, events

logger = logging.getLogger(__name__)


@dataclass
class IncomingMessage:
    telegram_msg_id: int
    chat_id: int
    sender_id: int | None
    sender_username: str | None
    text: str
    date: str


class Collector:
    """Listens for new messages in monitored channels and pushes to a queue."""

    def __init__(self, client: TelegramClient, queue: asyncio.Queue[IncomingMessage]) -> None:
        self._client = client
        self._queue = queue
        self._monitored_chats: set[int] = set()

    async def subscribe(self, chat_ids: list[int]) -> None:
        self._monitored_chats = set(chat_ids)
        logger.info("Collector subscribed to %d channels", len(chat_ids))

        @self._client.on(events.NewMessage)
        async def handler(event: events.NewMessage.Event) -> None:
            if event.chat_id not in self._monitored_chats:
                return
            msg = event.message
            if not msg.text:
                return
            sender = await msg.get_sender()
            sender_id = sender.id if sender else None
            sender_username = getattr(sender, "username", None)
            incoming = IncomingMessage(
                telegram_msg_id=msg.id,
                chat_id=msg.chat_id,
                sender_id=sender_id,
                sender_username=sender_username,
                text=msg.text,
                date=msg.date.astimezone(timezone.utc).isoformat() if msg.date else datetime.now(timezone.utc).isoformat(),
            )
            await self._queue.put(incoming)
            logger.debug("Queued message %d from chat %d", msg.id, msg.chat_id)

    def add_chat(self, chat_id: int) -> None:
        """Hot-subscribe to a new channel at runtime."""
        if chat_id not in self._monitored_chats:
            self._monitored_chats.add(chat_id)
            logger.info("Hot-subscribed to chat %d (total: %d)", chat_id, len(self._monitored_chats))

    def remove_chat(self, chat_id: int) -> None:
        """Stop monitoring a channel at runtime."""
        self._monitored_chats.discard(chat_id)
        logger.info("Unsubscribed from chat %d (total: %d)", chat_id, len(self._monitored_chats))

    async def backfill(self, chat_id: int, limit: int = 50) -> None:
        """Fetch recent messages from a channel for initial processing."""
        async for msg in self._client.iter_messages(chat_id, limit=limit):
            if not msg.text:
                continue
            sender = await msg.get_sender()
            sender_id = sender.id if sender else None
            sender_username = getattr(sender, "username", None)
            incoming = IncomingMessage(
                telegram_msg_id=msg.id,
                chat_id=chat_id,
                sender_id=sender_id,
                sender_username=sender_username,
                text=msg.text,
                date=msg.date.astimezone(timezone.utc).isoformat() if msg.date else datetime.now(timezone.utc).isoformat(),
            )
            await self._queue.put(incoming)
        logger.info("Backfilled %d messages from chat %d", limit, chat_id)
