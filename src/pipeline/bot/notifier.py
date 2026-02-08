from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from telethon import TelegramClient, events
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)

StatusHandler = Callable[[], Awaitable[str]]


class Notifier:
    """Sends notifications and handles status commands via a Telegram bot."""

    def __init__(self, bot_token: str, chat_id: int, api_id: int, api_hash: str) -> None:
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._api_id = api_id
        self._api_hash = api_hash
        self._bot: TelegramClient | None = None
        self._command_handlers: dict[str, StatusHandler] = {}

    def register_command(self, command: str, handler: StatusHandler) -> None:
        """Register a handler for a bot command (e.g. 'status' for /status)."""
        self._command_handlers[command] = handler

    async def start(self) -> None:
        self._bot = TelegramClient(StringSession(), api_id=self._api_id, api_hash=self._api_hash)
        await self._bot.start(bot_token=self._bot_token)
        logger.info("Notifier bot started, target chat=%d", self._chat_id)

        @self._bot.on(events.NewMessage(pattern=r"/\w+"))
        async def on_command(event: events.NewMessage.Event) -> None:
            if event.chat_id != self._chat_id:
                return
            text = event.raw_text.strip()
            cmd = text.split()[0].lstrip("/").split("@")[0]
            handler = self._command_handlers.get(cmd)
            if handler:
                try:
                    response = await handler()
                    await event.reply(response)
                except Exception:
                    logger.exception("Error handling /%s", cmd)
                    await event.reply(f"Error processing /{cmd}")
            elif cmd == "help":
                cmds = ", ".join(f"/{c}" for c in sorted(self._command_handlers))
                await event.reply(f"Available commands: {cmds}, /help")

    async def stop(self) -> None:
        if self._bot:
            await self._bot.disconnect()

    async def send(self, text: str) -> None:
        if not self._bot:
            logger.warning("Notifier not started")
            return
        await self._bot.send_message(self._chat_id, text)

    async def notify_positive_reply(
        self,
        lead_summary: str,
        reply_text: str,
        sender_username: str | None,
        channel_title: str | None,
    ) -> None:
        username_str = f"@{sender_username}" if sender_username else "unknown"
        channel_str = channel_title or "unknown channel"
        text = (
            f"**Positive reply!**\n\n"
            f"Channel: {channel_str}\n"
            f"From: {username_str}\n\n"
            f"Order: {lead_summary}\n\n"
            f"Reply:\n{reply_text}"
        )
        await self.send(text)

    async def notify_peer_flood(self) -> None:
        await self.send(
            "**PeerFloodError** — outreach paused for 24 hours.\n"
            "Check account safety."
        )

    async def send_daily_summary(self, stats: dict[str, int]) -> None:
        lines = ["**Daily Summary**\n"]
        for status, count in sorted(stats.items()):
            lines.append(f"  {status}: {count}")
        await self.send("\n".join(lines))
