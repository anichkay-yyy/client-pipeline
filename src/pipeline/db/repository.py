from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import aiosqlite

from .models import Channel, DailyBudget, Lead, Message, Reply

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Repository:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        schema = _SCHEMA_PATH.read_text()
        await self._db.executescript(schema)
        await self._db.commit()
        await self._migrate(self._db)
        logger.info("Database connected: %s", self._db_path)

    @staticmethod
    async def _migrate(db: aiosqlite.Connection) -> None:
        # channels migrations
        cur = await db.execute("PRAGMA table_info(channels)")
        ch_cols = {r[1] for r in await cur.fetchall()}
        for col in ("discovered_from", "discovered_at"):
            if col not in ch_cols:
                await db.execute(f"ALTER TABLE channels ADD COLUMN {col} TEXT")
                logger.info("Migration: added channels.%s", col)
        # messages migrations
        cur = await db.execute("PRAGMA table_info(messages)")
        msg_cols = {r[1] for r in await cur.fetchall()}
        if "text_hash" not in msg_cols:
            await db.execute("ALTER TABLE messages ADD COLUMN text_hash TEXT")
            logger.info("Migration: added messages.text_hash")
        await db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Call connect() first"
        return self._db

    # ── channels ──

    async def upsert_channel(self, ch: Channel) -> int:
        await self.db.execute(
            """INSERT INTO channels (telegram_id, username, title, is_active,
                                    discovered_from, discovered_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(telegram_id) DO UPDATE SET
                   username=excluded.username,
                   title=excluded.title,
                   is_active=excluded.is_active""",
            (ch.telegram_id, ch.username, ch.title, int(ch.is_active),
             ch.discovered_from, ch.discovered_at),
        )
        await self.db.commit()
        cur = await self.db.execute(
            "SELECT id FROM channels WHERE telegram_id=?", (ch.telegram_id,)
        )
        row = await cur.fetchone()
        return row["id"]

    async def get_active_channels(self) -> list[Channel]:
        cur = await self.db.execute("SELECT * FROM channels WHERE is_active=1")
        rows = await cur.fetchall()
        return [self._row_to_channel(r) for r in rows]

    async def get_channel_by_telegram_id(self, telegram_id: int) -> Channel | None:
        cur = await self.db.execute(
            "SELECT * FROM channels WHERE telegram_id=?", (telegram_id,)
        )
        r = await cur.fetchone()
        if not r:
            return None
        return self._row_to_channel(r)

    async def count_active_channels(self) -> int:
        cur = await self.db.execute("SELECT COUNT(*) as cnt FROM channels WHERE is_active=1")
        row = await cur.fetchone()
        return row["cnt"]

    @staticmethod
    def _row_to_channel(r: aiosqlite.Row) -> Channel:
        return Channel(
            id=r["id"],
            telegram_id=r["telegram_id"],
            username=r["username"],
            title=r["title"],
            is_active=bool(r["is_active"]),
            discovered_from=r["discovered_from"],
            discovered_at=r["discovered_at"],
        )

    async def deactivate_channel(self, channel_id: int) -> None:
        await self.db.execute(
            "UPDATE channels SET is_active=0 WHERE id=?", (channel_id,)
        )
        await self.db.commit()

    async def get_dead_channels(self, min_age_days: int = 7) -> list[Channel]:
        """Return active channels older than min_age_days that have produced zero leads."""
        cur = await self.db.execute(
            """SELECT c.* FROM channels c
               WHERE c.is_active = 1
                 AND c.discovered_at IS NOT NULL
                 AND julianday('now') - julianday(c.discovered_at) > ?
                 AND NOT EXISTS (
                     SELECT 1 FROM messages m
                     JOIN leads l ON l.message_id = m.id
                     WHERE m.channel_id = c.id
                 )
               ORDER BY c.id""",
            (min_age_days,),
        )
        rows = await cur.fetchall()
        return [self._row_to_channel(r) for r in rows]

    # ── messages ──

    async def insert_message(self, msg: Message) -> int | None:
        """Insert message. Returns id on success, None on duplicate."""
        try:
            cur = await self.db.execute(
                """INSERT INTO messages
                   (telegram_msg_id, channel_id, sender_id, sender_username, text, date, text_hash)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    msg.telegram_msg_id,
                    msg.channel_id,
                    msg.sender_id,
                    msg.sender_username,
                    msg.text,
                    msg.date,
                    msg.text_hash,
                ),
            )
            await self.db.commit()
            return cur.lastrowid
        except aiosqlite.IntegrityError:
            return None

    async def has_lead_with_text_hash(self, text_hash: str) -> bool:
        """Check if a lead already exists for a message with the same text_hash."""
        cur = await self.db.execute(
            """SELECT 1 FROM leads l
               JOIN messages m ON l.message_id = m.id
               WHERE m.text_hash = ?
               LIMIT 1""",
            (text_hash,),
        )
        return await cur.fetchone() is not None

    async def get_message(self, message_id: int) -> Message | None:
        cur = await self.db.execute("SELECT * FROM messages WHERE id=?", (message_id,))
        r = await cur.fetchone()
        if not r:
            return None
        return Message(
            id=r["id"],
            telegram_msg_id=r["telegram_msg_id"],
            channel_id=r["channel_id"],
            sender_id=r["sender_id"],
            sender_username=r["sender_username"],
            text=r["text"],
            date=r["date"],
            text_hash=r["text_hash"],
        )

    # ── leads ──

    async def insert_lead(self, lead: Lead) -> int:
        cur = await self.db.execute(
            """INSERT INTO leads
               (message_id, status, relevance_score, budget, stack,
                deadline, language, summary)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                lead.message_id,
                lead.status,
                lead.relevance_score,
                lead.budget,
                lead.stack,
                lead.deadline,
                lead.language,
                lead.summary,
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_leads_by_status(self, status: str) -> list[Lead]:
        cur = await self.db.execute(
            "SELECT * FROM leads WHERE status=? ORDER BY id", (status,)
        )
        rows = await cur.fetchall()
        return [self._row_to_lead(r) for r in rows]

    async def get_lead(self, lead_id: int) -> Lead | None:
        cur = await self.db.execute("SELECT * FROM leads WHERE id=?", (lead_id,))
        r = await cur.fetchone()
        if not r:
            return None
        return self._row_to_lead(r)

    async def update_lead(self, lead: Lead) -> None:
        await self.db.execute(
            """UPDATE leads SET
                   status=?, relevance_score=?, budget=?, stack=?,
                   deadline=?, language=?, summary=?,
                   outreach_text=?, dm_text=?,
                   outreach_msg_id=?, dm_msg_id=?,
                   contacted_at=?, replied_at=?, forwarded_at=?
               WHERE id=?""",
            (
                lead.status,
                lead.relevance_score,
                lead.budget,
                lead.stack,
                lead.deadline,
                lead.language,
                lead.summary,
                lead.outreach_text,
                lead.dm_text,
                lead.outreach_msg_id,
                lead.dm_msg_id,
                lead.contacted_at,
                lead.replied_at,
                lead.forwarded_at,
                lead.id,
            ),
        )
        await self.db.commit()

    async def get_stale_leads(self, ttl_hours: int) -> list[Lead]:
        cutoff = datetime.now(timezone.utc).isoformat()
        cur = await self.db.execute(
            """SELECT * FROM leads
               WHERE status='contacted'
                 AND contacted_at IS NOT NULL
                 AND julianday(?) - julianday(contacted_at) > ?/24.0
               ORDER BY id""",
            (cutoff, ttl_hours),
        )
        rows = await cur.fetchall()
        return [self._row_to_lead(r) for r in rows]

    async def get_daily_stats(self) -> dict[str, int]:
        cur = await self.db.execute(
            "SELECT status, COUNT(*) as cnt FROM leads GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in await cur.fetchall()}

    async def get_full_stats(self) -> dict[str, int]:
        """Return comprehensive stats for status monitoring."""
        stats: dict[str, int] = {}
        # channels
        cur = await self.db.execute("SELECT COUNT(*) as cnt FROM channels WHERE is_active=1")
        stats["channels_active"] = (await cur.fetchone())["cnt"]
        cur = await self.db.execute("SELECT COUNT(*) as cnt FROM channels WHERE is_active=0")
        stats["channels_inactive"] = (await cur.fetchone())["cnt"]
        # messages
        cur = await self.db.execute("SELECT COUNT(*) as cnt FROM messages")
        stats["messages_total"] = (await cur.fetchone())["cnt"]
        today = date.today().isoformat()
        cur = await self.db.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE date >= ?", (today,)
        )
        stats["messages_today"] = (await cur.fetchone())["cnt"]
        # leads by status
        cur = await self.db.execute(
            "SELECT status, COUNT(*) as cnt FROM leads GROUP BY status"
        )
        for r in await cur.fetchall():
            stats[f"leads_{r['status']}"] = r["cnt"]
        cur = await self.db.execute("SELECT COUNT(*) as cnt FROM leads")
        stats["leads_total"] = (await cur.fetchone())["cnt"]
        # daily budget
        cur = await self.db.execute(
            "SELECT sends_used, max_sends FROM daily_budget WHERE date=?", (today,)
        )
        r = await cur.fetchone()
        if r:
            stats["sends_today"] = r["sends_used"]
            stats["sends_limit"] = r["max_sends"]
        else:
            stats["sends_today"] = 0
            stats["sends_limit"] = 0
        return stats

    @staticmethod
    def _row_to_lead(r: aiosqlite.Row) -> Lead:
        return Lead(
            id=r["id"],
            message_id=r["message_id"],
            status=r["status"],
            relevance_score=r["relevance_score"],
            budget=r["budget"],
            stack=r["stack"],
            deadline=r["deadline"],
            language=r["language"],
            summary=r["summary"],
            outreach_text=r["outreach_text"],
            dm_text=r["dm_text"],
            outreach_msg_id=r["outreach_msg_id"],
            dm_msg_id=r["dm_msg_id"],
            contacted_at=r["contacted_at"],
            replied_at=r["replied_at"],
            forwarded_at=r["forwarded_at"],
        )

    # ── replies ──

    async def insert_reply(self, reply: Reply) -> int:
        cur = await self.db.execute(
            """INSERT INTO replies
               (lead_id, telegram_msg_id, sender_id, text, sentiment, received_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                reply.lead_id,
                reply.telegram_msg_id,
                reply.sender_id,
                reply.text,
                reply.sentiment,
                reply.received_at,
            ),
        )
        await self.db.commit()
        return cur.lastrowid  # type: ignore[return-value]

    async def get_replies_for_lead(self, lead_id: int) -> list[Reply]:
        cur = await self.db.execute(
            "SELECT * FROM replies WHERE lead_id=? ORDER BY id", (lead_id,)
        )
        rows = await cur.fetchall()
        return [
            Reply(
                id=r["id"],
                lead_id=r["lead_id"],
                telegram_msg_id=r["telegram_msg_id"],
                sender_id=r["sender_id"],
                text=r["text"],
                sentiment=r["sentiment"],
                received_at=r["received_at"],
            )
            for r in rows
        ]

    # ── daily_budget ──

    async def get_daily_budget(self, max_sends: int) -> DailyBudget:
        today = date.today().isoformat()
        await self.db.execute(
            """INSERT INTO daily_budget (date, sends_used, max_sends)
               VALUES (?, 0, ?)
               ON CONFLICT(date) DO NOTHING""",
            (today, max_sends),
        )
        await self.db.commit()
        cur = await self.db.execute(
            "SELECT * FROM daily_budget WHERE date=?", (today,)
        )
        r = await cur.fetchone()
        return DailyBudget(
            date=r["date"], sends_used=r["sends_used"], max_sends=r["max_sends"]
        )

    async def increment_daily_sends(self) -> None:
        today = date.today().isoformat()
        await self.db.execute(
            "UPDATE daily_budget SET sends_used = sends_used + 1 WHERE date=?",
            (today,),
        )
        await self.db.commit()
