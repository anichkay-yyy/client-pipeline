from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from telethon import TelegramClient, events

from ..db.models import Lead, Reply
from ..db.repository import Repository
from ..llm.openrouter import OpenRouterClient
from ..llm.sentiment import analyze_sentiment

logger = logging.getLogger(__name__)


class Responder:
    """Monitors replies to our outreach messages (DM + thread polling)."""

    def __init__(
        self,
        client: TelegramClient,
        repo: Repository,
        llm: OpenRouterClient,
    ) -> None:
        self._client = client
        self._repo = repo
        self._llm = llm
        self._our_user_id: int | None = None

    async def init(self) -> None:
        me = await self._client.get_me()
        self._our_user_id = me.id  # type: ignore[union-attr]

    def register_dm_listener(self, callback: asyncio.coroutines) -> None:  # type: ignore[type-arg]
        """Register handler for incoming DMs that might be replies to our outreach."""

        @self._client.on(events.NewMessage(incoming=True, func=lambda e: e.is_private))
        async def handler(event: events.NewMessage.Event) -> None:
            msg = event.message
            sender_id = msg.sender_id
            if sender_id == self._our_user_id:
                return

            # Find if this sender has a contacted lead
            leads = await self._repo.get_leads_by_status("contacted")
            for lead in leads:
                original = await self._repo.get_message(lead.message_id)
                if original and original.sender_id == sender_id:
                    await self._process_reply(lead, msg.text, msg.id, sender_id)
                    break

    async def poll_thread_replies(self) -> None:
        """Check threads of contacted leads for new replies."""
        leads = await self._repo.get_leads_by_status("contacted")
        for lead in leads:
            if not lead.outreach_msg_id:
                continue
            original = await self._repo.get_message(lead.message_id)
            if not original:
                continue

            channel = await self._repo.get_channel_by_telegram_id(0)
            # Get channel from message's channel_id
            from ..db.models import Channel
            cur = await self._repo.db.execute(
                "SELECT * FROM channels WHERE id=?", (original.channel_id,)
            )
            row = await cur.fetchone()
            if not row:
                continue

            try:
                async for msg in self._client.iter_messages(
                    row["telegram_id"],
                    reply_to=lead.outreach_msg_id,
                    limit=10,
                ):
                    if not msg.text or msg.sender_id == self._our_user_id:
                        continue
                    # Check we haven't already processed this reply
                    existing = await self._repo.get_replies_for_lead(lead.id)  # type: ignore[arg-type]
                    if any(r.telegram_msg_id == msg.id for r in existing):
                        continue
                    await self._process_reply(lead, msg.text, msg.id, msg.sender_id)
            except Exception as e:
                logger.warning("Error polling thread for lead %d: %s", lead.id, e)

    async def _process_reply(
        self, lead: Lead, text: str, msg_id: int, sender_id: int
    ) -> None:
        outreach_text = lead.outreach_text or lead.dm_text or ""
        result = await analyze_sentiment(self._llm, outreach_text, text)

        sentiment = "unclear"
        if result:
            sentiment = result.get("sentiment", "unclear")

        reply = Reply(
            lead_id=lead.id,  # type: ignore[arg-type]
            telegram_msg_id=msg_id,
            sender_id=sender_id,
            text=text,
            sentiment=sentiment,
            received_at=datetime.now(timezone.utc).isoformat(),
        )
        await self._repo.insert_reply(reply)

        lead.status = "replied"
        lead.replied_at = datetime.now(timezone.utc).isoformat()
        await self._repo.update_lead(lead)

        logger.info(
            "Reply from %d for lead %d: sentiment=%s",
            sender_id, lead.id, sentiment,
        )
