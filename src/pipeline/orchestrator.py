from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime, time, timezone

from .bot.notifier import Notifier
from .config import Settings
from .db.models import Channel, Lead, Message
from .db.repository import Repository
from .llm.classifier import classify_message
from .llm.openrouter import OpenRouterClient
from .llm.outreach import generate_dm, generate_thread_reply
from .telegram.collector import Collector, IncomingMessage
from .telegram.discovery import DiscoveryService
from .telegram.responder import Responder
from .telegram.sender import Sender
from .utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(
        self,
        settings: Settings,
        repo: Repository,
        llm: OpenRouterClient,
        collector: Collector,
        sender: Sender,
        responder: Responder,
        notifier: Notifier,
        queue: asyncio.Queue[IncomingMessage],
        discovery: DiscoveryService | None = None,
    ) -> None:
        self._s = settings
        self._repo = repo
        self._llm = llm
        self._collector = collector
        self._sender = sender
        self._responder = responder
        self._notifier = notifier
        self._queue = queue
        self._discovery = discovery
        self._rate_limiter = RateLimiter(
            min_delay=settings.rate_min_delay,
            max_delay=settings.rate_max_delay,
            warmup_schedule={
                "week_1": settings.rate_warmup_week_1,
                "week_2": settings.rate_warmup_week_2,
                "week_3": settings.rate_warmup_week_3,
                "week_4_plus": settings.rate_warmup_week_4_plus,
            },
        )
        self._outreach_paused_until: datetime | None = None
        self._running = True
        self._started_at: datetime | None = None

    async def run(self) -> None:
        self._started_at = datetime.now(timezone.utc)
        tasks = [
            asyncio.create_task(self._processor_loop(), name="processor"),
            asyncio.create_task(self._outreach_loop(), name="outreach"),
            asyncio.create_task(self._reply_poller(), name="reply_poller"),
            asyncio.create_task(self._janitor_loop(), name="janitor"),
            asyncio.create_task(self._summary_loop(), name="summary"),
        ]
        if self._discovery:
            tasks.append(asyncio.create_task(self._discovery_loop(), name="discovery"))
        logger.info("Orchestrator starting %d concurrent tasks", len(tasks))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Orchestrator shutting down")
        except Exception:
            logger.exception("Orchestrator fatal error")
            for t in tasks:
                t.cancel()
            raise

    def stop(self) -> None:
        self._running = False

    async def get_status(self) -> str:
        """Build a status report string for monitoring."""
        now = datetime.now(timezone.utc)
        uptime = str(now - self._started_at).split(".")[0] if self._started_at else "N/A"

        stats = await self._repo.get_full_stats()
        monitored = len(self._collector._monitored_chats)
        queue_size = self._queue.qsize()

        paused = "yes" if self._is_outreach_paused() else "no"
        paused_until = ""
        if self._outreach_paused_until:
            paused_until = f" (until {self._outreach_paused_until.strftime('%H:%M UTC')})"

        lines = [
            "** Pipeline Status**",
            "",
            f"Uptime: {uptime}",
            f"Queue: {queue_size} messages pending",
            f"Discovery: {'on' if self._discovery else 'off'}",
            "",
            "**Channels**",
            f"  Monitoring: {monitored}",
            f"  Active (DB): {stats.get('channels_active', 0)}",
            f"  Deactivated: {stats.get('channels_inactive', 0)}",
            "",
            "**Messages**",
            f"  Total: {stats.get('messages_total', 0)}",
            f"  Today: {stats.get('messages_today', 0)}",
            "",
            "**Leads**",
            f"  Total: {stats.get('leads_total', 0)}",
            f"  New: {stats.get('leads_new', 0)}",
            f"  Contacted: {stats.get('leads_contacted', 0)}",
            f"  Replied: {stats.get('leads_replied', 0)}",
            f"  Forwarded: {stats.get('leads_forwarded', 0)}",
            f"  Negative: {stats.get('leads_negative', 0)}",
            f"  No reply: {stats.get('leads_no_reply', 0)}",
            "",
            "**Outreach**",
            f"  Sends today: {stats.get('sends_today', 0)}/{stats.get('sends_limit', 0)}",
            f"  Paused: {paused}{paused_until}",
        ]
        return "\n".join(lines)

    async def get_channels_report(self) -> str:
        """Build a channels report for /channels command."""
        channels = await self._repo.get_active_channels()
        if not channels:
            return "No active channels."
        lines = [f"**Active Channels** ({len(channels)})", ""]
        for ch in channels:
            name = ch.title or ch.username or str(ch.telegram_id)
            source = ch.discovered_from or "seed"
            lines.append(f"  {name} — {source}")
        return "\n".join(lines)

    async def get_leads_report(self, limit: int = 10) -> str:
        """Build a recent leads report for /leads command."""
        cur = await self._repo.db.execute(
            "SELECT * FROM leads ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        if not rows:
            return "No leads yet."
        lines = [f"**Recent Leads** (last {limit})", ""]
        for r in rows:
            score = r["relevance_score"] or 0
            status = r["status"]
            summary = (r["summary"] or "")[:80]
            lines.append(f"  #{r['id']} [{status}] score={score:.1f} — {summary}")
        return "\n".join(lines)

    # ── 1. Processor: queue → DB → classify → create leads ──

    async def _processor_loop(self) -> None:
        logger.info("Processor loop started")
        while self._running:
            try:
                incoming = await asyncio.wait_for(
                    self._queue.get(), timeout=self._s.orchestrator_cycle_interval
                )
            except asyncio.TimeoutError:
                continue

            try:
                await self._process_message(incoming)
            except Exception:
                logger.exception("Error processing message %d", incoming.telegram_msg_id)

    @staticmethod
    def _compute_text_hash(text: str) -> str:
        """Normalize text and compute SHA-256 hash for cross-channel dedup."""
        normalized = re.sub(r"\s+", " ", text.strip().lower())
        return hashlib.sha256(normalized.encode()).hexdigest()

    async def _process_message(self, incoming: IncomingMessage) -> None:
        channel = await self._repo.get_channel_by_telegram_id(incoming.chat_id)
        if not channel:
            logger.warning("Unknown channel %d, skipping", incoming.chat_id)
            return

        text_hash = self._compute_text_hash(incoming.text)
        msg = Message(
            telegram_msg_id=incoming.telegram_msg_id,
            channel_id=channel.id,  # type: ignore[arg-type]
            sender_id=incoming.sender_id,
            sender_username=incoming.sender_username,
            text=incoming.text,
            date=incoming.date,
            text_hash=text_hash,
        )
        msg_id = await self._repo.insert_message(msg)
        if msg_id is None:
            return  # duplicate

        # Cross-channel dedup: skip if a lead already exists for the same text
        if await self._repo.has_lead_with_text_hash(text_hash):
            logger.debug("Duplicate text detected (hash=%s…), skipping lead", text_hash[:12])
            return

        result = await classify_message(
            self._llm, incoming.text, self._s.classification_target_stacks
        )
        if result is None:
            return

        score = result.get("relevance_score", 0)
        if score < self._s.classification_min_relevance:
            logger.debug("Low relevance %.2f, skipping", score)
            return

        lead = Lead(
            message_id=msg_id,
            status="new",
            relevance_score=score,
            budget=result.get("budget"),
            stack=result.get("stack"),
            deadline=result.get("deadline"),
            language=result.get("language"),
            summary=result.get("summary"),
        )
        lead_id = await self._repo.insert_lead(lead)
        logger.info("Created lead %d (score=%.2f, lang=%s)", lead_id, score, lead.language)

    # ── 2. Outreach: leads (new) → generate text → send ──

    async def _outreach_loop(self) -> None:
        logger.info("Outreach loop started")
        while self._running:
            if self._is_outreach_paused():
                await asyncio.sleep(60)
                continue

            budget = await self._repo.get_daily_budget(
                self._rate_limiter.get_current_daily_limit()
            )
            if budget.sends_used >= budget.max_sends:
                logger.info("Daily budget exhausted (%d/%d)", budget.sends_used, budget.max_sends)
                await asyncio.sleep(300)
                continue

            leads = await self._repo.get_leads_by_status("new")
            if not leads:
                await asyncio.sleep(self._s.orchestrator_cycle_interval)
                continue

            for lead in leads:
                if self._is_outreach_paused():
                    break

                budget = await self._repo.get_daily_budget(
                    self._rate_limiter.get_current_daily_limit()
                )
                if budget.sends_used >= budget.max_sends:
                    break

                try:
                    await self._send_outreach(lead)
                except Exception:
                    logger.exception("Error sending outreach for lead %d", lead.id)

                await self._rate_limiter.wait()

    async def _send_outreach(self, lead: Lead) -> None:
        msg = await self._repo.get_message(lead.message_id)
        if not msg or not msg.text:
            return

        cur = await self._repo.db.execute(
            "SELECT * FROM channels WHERE id=?", (msg.channel_id,)
        )
        ch_row = await cur.fetchone()
        if not ch_row:
            return

        language = lead.language or "en"
        channel_title = ch_row["title"] or ch_row["username"] or "channel"

        thread_text = await generate_thread_reply(
            self._llm, msg.text, language,
            temperature=self._s.llm_temperature,
            max_tokens=self._s.llm_max_tokens,
        )
        dm_text = await generate_dm(
            self._llm, msg.text, language, channel_title,
            temperature=self._s.llm_temperature,
            max_tokens=300,
        )

        result = await self._sender.send_outreach(
            chat_id=ch_row["telegram_id"],
            reply_to_msg_id=msg.telegram_msg_id,
            thread_text=thread_text,
            user_id=msg.sender_id,
            dm_text=dm_text,
        )

        if result.peer_flood:
            await self._pause_outreach(hours=24)
            await self._notifier.notify_peer_flood()
            return

        lead.outreach_text = thread_text
        lead.dm_text = dm_text
        lead.outreach_msg_id = result.thread_msg_id
        lead.dm_msg_id = result.dm_msg_id
        lead.status = "contacted"
        lead.contacted_at = datetime.now(timezone.utc).isoformat()
        await self._repo.update_lead(lead)
        await self._repo.increment_daily_sends()

        logger.info("Outreach sent for lead %d", lead.id)

    def _is_outreach_paused(self) -> bool:
        if self._outreach_paused_until is None:
            return False
        if datetime.now(timezone.utc) >= self._outreach_paused_until:
            self._outreach_paused_until = None
            logger.info("Outreach pause lifted")
            return False
        return True

    async def _pause_outreach(self, hours: int) -> None:
        from datetime import timedelta
        self._outreach_paused_until = datetime.now(timezone.utc) + timedelta(hours=hours)
        logger.warning("Outreach paused until %s", self._outreach_paused_until.isoformat())

    # ── 3. Reply poller: check threads + DM listener → sentiment → forward ──

    async def _reply_poller(self) -> None:
        logger.info("Reply poller started")
        self._responder.register_dm_listener(self._on_reply_detected)

        while self._running:
            try:
                await self._responder.poll_thread_replies()
                await self._forward_positive_replies()
            except Exception:
                logger.exception("Error in reply poller")
            await asyncio.sleep(self._s.response_check_interval)

    async def _on_reply_detected(self) -> None:
        await self._forward_positive_replies()

    async def _forward_positive_replies(self) -> None:
        leads = await self._repo.get_leads_by_status("replied")
        for lead in leads:
            replies = await self._repo.get_replies_for_lead(lead.id)  # type: ignore[arg-type]
            for reply in replies:
                if reply.sentiment == "positive":
                    msg = await self._repo.get_message(lead.message_id)
                    cur = await self._repo.db.execute(
                        "SELECT * FROM channels WHERE id=?", (msg.channel_id,)
                    ) if msg else None
                    ch_row = await cur.fetchone() if cur else None
                    channel_title = ch_row["title"] if ch_row else None

                    await self._notifier.notify_positive_reply(
                        lead_summary=lead.summary or "",
                        reply_text=reply.text or "",
                        sender_username=msg.sender_username if msg else None,
                        channel_title=channel_title,
                    )
                    lead.status = "forwarded"
                    lead.forwarded_at = datetime.now(timezone.utc).isoformat()
                    await self._repo.update_lead(lead)
                    break
            else:
                # No positive reply — check if any negative
                if any(r.sentiment == "negative" for r in replies):
                    lead.status = "negative"
                    await self._repo.update_lead(lead)

    # ── 4. Janitor: mark stale leads + deactivate dead channels ──

    async def _janitor_loop(self) -> None:
        logger.info("Janitor loop started")
        while self._running:
            try:
                stale = await self._repo.get_stale_leads(
                    self._s.response_no_reply_ttl_hours
                )
                for lead in stale:
                    lead.status = "no_reply"
                    await self._repo.update_lead(lead)
                    logger.info("Lead %d marked as no_reply (stale)", lead.id)
            except Exception:
                logger.exception("Error in janitor loop (stale leads)")

            try:
                dead = await self._repo.get_dead_channels(min_age_days=7)
                for ch in dead:
                    await self._repo.deactivate_channel(ch.id)  # type: ignore[arg-type]
                    if ch.telegram_id:
                        self._collector.remove_chat(ch.telegram_id)
                    logger.info(
                        "Deactivated dead channel: %s (id=%d, db_id=%d)",
                        ch.title or ch.username or "?", ch.telegram_id or 0, ch.id or 0,
                    )
                if dead:
                    logger.info("Janitor deactivated %d dead channels", len(dead))
            except Exception:
                logger.exception("Error in janitor loop (dead channels)")

            await asyncio.sleep(3600)

    # ── 5. Summary: daily report at 21:00 ──

    async def _summary_loop(self) -> None:
        logger.info("Summary loop started")
        while self._running:
            now = datetime.now(timezone.utc)
            target = datetime.combine(now.date(), time(21, 0), tzinfo=timezone.utc)
            if now >= target:
                from datetime import timedelta
                target += timedelta(days=1)
            wait_secs = (target - now).total_seconds()
            logger.debug("Next summary in %.0f seconds", wait_secs)
            await asyncio.sleep(wait_secs)

            try:
                stats = await self._repo.get_daily_stats()
                await self._notifier.send_daily_summary(stats)
                logger.info("Daily summary sent: %s", stats)
            except Exception:
                logger.exception("Error sending daily summary")

    # ── 6. Discovery: find new channels automatically ──

    async def _discovery_loop(self) -> None:
        assert self._discovery is not None
        logger.info("Discovery loop started (interval=%ds)", self._s.discovery_interval)
        # keyword search already ran at startup; wait before first full cycle
        await asyncio.sleep(self._s.discovery_interval)

        while self._running:
            try:
                count = await self._discovery.run_cycle()
                logger.info("Discovery cycle complete: %d new channels", count)
            except Exception:
                logger.exception("Error in discovery cycle")
            await asyncio.sleep(self._s.discovery_interval)
