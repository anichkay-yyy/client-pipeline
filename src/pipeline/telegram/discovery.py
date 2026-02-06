from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel as TgChannel, PeerChannel

from ..config import Settings
from ..db.models import Channel
from ..db.repository import Repository
from ..llm.openrouter import OpenRouterClient
from .collector import Collector

logger = logging.getLogger(__name__)

_USERNAME_RE = re.compile(r"@([A-Za-z]\w{4,31})")

_API_DELAY = 2.0  # seconds between Telegram API calls to avoid FloodWait

_FALLBACK_KEYWORDS = [
    "freelance orders",
    "freelance jobs",
    "заказы фриланс",
    "фриланс заказы",
    "ищу разработчика",
    "ищу программиста",
    "web developer needed",
    "разработка заказы",
    "freelance developer",
    "замовлення фріланс",
    "trabajo freelance",
    "Freelance Aufträge",
    "commandes freelance",
    "pedidos freelance",
    "zlecenia freelance",
]

_KEYWORD_GEN_PROMPT = """\
You generate search queries to find Telegram channels that post freelance orders \
(clients looking to hire developers).

Target technology stacks: {stacks}
Languages to generate keywords in: {languages}
Generate {per_lang} search queries PER language.

Rules:
- Each query is 2-4 words, optimized for Telegram's global search
- Mix: generic freelance terms, stack-specific queries, "looking for developer" phrases
- Vary phrasing: don't just translate the same phrase into each language
- No duplicates, no hashtags, no @ mentions

Return ONLY a JSON object:
{{"keywords": ["query1", "query2", ...]}}"""

_CHANNEL_VALIDATION_PROMPT = """\
You are evaluating whether a Telegram channel posts genuine freelance orders \
(clients looking to hire developers/designers/etc).

Here are the most recent messages from the channel:

{messages}

Based on these messages, is this channel a source of freelance orders?

Rules:
- "Yes" if at least 30% of messages are real client orders (hiring posts)
- "No" if the channel is mostly: news, memes, self-promotion, job-seeking, ads, crypto, spam

Return ONLY a JSON object:
{{"is_relevant": true/false, "reason": "brief explanation"}}"""


class DiscoveryService:
    """Discovers new freelance channels via keyword search, forwarded messages, and descriptions."""

    def __init__(
        self,
        client: TelegramClient,
        repo: Repository,
        collector: Collector,
        settings: Settings,
        llm: OpenRouterClient,
    ) -> None:
        self._client = client
        self._repo = repo
        self._collector = collector
        self._s = settings
        self._llm = llm
        self._known_ids: set[int] = set()
        self._rejected_ids: set[int] = set()
        self._generated_keywords: list[str] = []

    async def init(self) -> None:
        """Load already-known channel IDs from DB and generate search keywords."""
        channels = await self._repo.get_active_channels()
        self._known_ids = {ch.telegram_id for ch in channels if ch.telegram_id}
        self._generated_keywords = await self._generate_keywords()
        logger.info(
            "Discovery init: %d known channels, %d keywords (%d generated + %d fallback)",
            len(self._known_ids),
            len(self._get_all_keywords()),
            len(self._generated_keywords),
            len(_FALLBACK_KEYWORDS),
        )

    async def _generate_keywords(self) -> list[str]:
        """Use LLM to generate search keywords based on target stacks and languages."""
        stacks = ", ".join(self._s.classification_target_stacks) or "web development, bots, automation"
        languages = ", ".join(self._s.discovery_keyword_languages)
        per_lang = self._s.discovery_keywords_per_language

        prompt = _KEYWORD_GEN_PROMPT.format(
            stacks=stacks, languages=languages, per_lang=per_lang,
        )
        try:
            result = await self._llm.chat_json(
                [{"role": "user", "content": prompt}],
                temperature=0.9,
                max_tokens=1500,
            )
            if result and "keywords" in result:
                keywords = [k.strip() for k in result["keywords"] if isinstance(k, str) and k.strip()]
                logger.info("LLM generated %d search keywords", len(keywords))
                return keywords
            logger.warning("LLM keyword generation returned unexpected format: %s", result)
        except Exception:
            logger.exception("Failed to generate keywords via LLM")
        return []

    def _get_all_keywords(self) -> list[str]:
        """Merge config + generated + fallback keywords, deduplicated."""
        combined = (
            self._s.discovery_search_keywords
            + self._generated_keywords
            + _FALLBACK_KEYWORDS
        )
        return list(dict.fromkeys(combined))

    async def run_cycle(self) -> int:
        """Run one full discovery cycle. Returns number of newly registered channels."""
        self._generated_keywords = await self._generate_keywords()
        registered = 0
        registered += await self.search_by_keywords()
        if self._s.discovery_scan_forwards:
            for chat_id in list(self._known_ids):
                registered += await self.extract_from_forwards(chat_id)
                if self._at_capacity():
                    break
        if self._s.discovery_scan_descriptions:
            for chat_id in list(self._known_ids):
                registered += await self.extract_from_description(chat_id)
                if self._at_capacity():
                    break
        return registered

    async def search_by_keywords(self) -> int:
        """Strategy 1: Search Telegram for channels matching keywords."""
        all_keywords = self._get_all_keywords()
        registered = 0
        for keyword in all_keywords:
            if self._at_capacity():
                break
            try:
                result = await self._client(
                    SearchRequest(q=keyword, limit=self._s.discovery_search_limit)
                )
                for chat in result.chats:
                    if not isinstance(chat, TgChannel) or chat.megagroup:
                        continue
                    added = await self._try_register(
                        chat.id, f"search:{keyword}",
                        username=getattr(chat, "username", None),
                        title=getattr(chat, "title", None),
                    )
                    if added:
                        registered += 1
            except FloodWaitError as e:
                logger.warning("FloodWait %ds during keyword search, pausing", e.seconds)
                await asyncio.sleep(e.seconds)
            except Exception:
                logger.exception("Discovery search failed for keyword: %s", keyword)
            await asyncio.sleep(_API_DELAY)
        return registered

    async def extract_from_forwards(self, chat_id: int) -> int:
        """Strategy 2: Scan recent messages for forwarded-from channels."""
        registered = 0
        try:
            async for msg in self._client.iter_messages(chat_id, limit=100):
                if self._at_capacity():
                    break
                fwd = msg.fwd_from
                if not fwd or not fwd.from_id:
                    continue
                if not isinstance(fwd.from_id, PeerChannel):
                    continue
                src_id = fwd.from_id.channel_id
                added = await self._try_register(src_id, f"forward:{chat_id}")
                if added:
                    registered += 1
        except FloodWaitError as e:
            logger.warning("FloodWait %ds during forward scan of %d", e.seconds, chat_id)
            await asyncio.sleep(e.seconds)
        except Exception:
            logger.exception("Discovery forward scan failed for chat %d", chat_id)
        await asyncio.sleep(_API_DELAY)
        return registered

    async def extract_from_description(self, chat_id: int) -> int:
        """Strategy 3: Parse channel description for @mentions of other channels."""
        registered = 0
        try:
            entity = await self._client.get_entity(chat_id)
            if not isinstance(entity, TgChannel):
                return 0
            full = await self._client(GetFullChannelRequest(entity))
            about = full.full_chat.about or ""
            usernames = _USERNAME_RE.findall(about)
            for uname in usernames:
                if self._at_capacity():
                    break
                try:
                    await asyncio.sleep(_API_DELAY)
                    target = await self._client.get_entity(uname)
                    if not isinstance(target, TgChannel) or target.megagroup:
                        continue
                    added = await self._try_register(
                        target.id, f"description:{chat_id}",
                        username=getattr(target, "username", None),
                        title=getattr(target, "title", None),
                    )
                    if added:
                        registered += 1
                except FloodWaitError as e:
                    logger.warning("FloodWait %ds resolving @%s", e.seconds, uname)
                    await asyncio.sleep(e.seconds)
                except Exception:
                    logger.debug("Could not resolve @%s from description", uname)
        except FloodWaitError as e:
            logger.warning("FloodWait %ds during description scan of %d", e.seconds, chat_id)
            await asyncio.sleep(e.seconds)
        except Exception:
            logger.exception("Discovery description scan failed for chat %d", chat_id)
        await asyncio.sleep(_API_DELAY)
        return registered

    async def _try_register(
        self,
        telegram_id: int,
        source: str,
        username: str | None = None,
        title: str | None = None,
    ) -> bool:
        """Deduplicate, validate, upsert, hot-subscribe, and backfill a new channel."""
        if telegram_id in self._known_ids or telegram_id in self._rejected_ids:
            return False
        if self._at_capacity():
            return False

        existing = await self._repo.get_channel_by_telegram_id(telegram_id)
        if existing:
            self._known_ids.add(telegram_id)
            return False

        # Resolve entity if needed
        if not username and not title:
            try:
                entity = await self._client.get_entity(telegram_id)
                username = getattr(entity, "username", None)
                title = getattr(entity, "title", None)
                await asyncio.sleep(_API_DELAY)
            except Exception:
                logger.debug("Could not resolve entity %d", telegram_id)
                return False

        # Validate channel content via LLM
        is_relevant = await self._validate_channel(telegram_id)
        if not is_relevant:
            self._rejected_ids.add(telegram_id)
            logger.info("Rejected channel %s (id=%d) — not relevant", title or username or "?", telegram_id)
            return False

        now = datetime.now(timezone.utc).isoformat()
        ch = Channel(
            telegram_id=telegram_id,
            username=username,
            title=title,
            discovered_from=source,
            discovered_at=now,
        )
        db_id = await self._repo.upsert_channel(ch)
        self._known_ids.add(telegram_id)
        self._collector.add_chat(telegram_id)

        if self._s.discovery_backfill_on_discover > 0:
            try:
                await self._collector.backfill(
                    telegram_id, limit=self._s.discovery_backfill_on_discover
                )
            except Exception:
                logger.exception("Backfill failed for discovered channel %d", telegram_id)

        logger.info(
            "Discovered channel: %s (id=%d, db_id=%d, source=%s)",
            title or username or "?", telegram_id, db_id, source,
        )
        return True

    async def _validate_channel(self, telegram_id: int) -> bool:
        """Check if a channel actually posts freelance orders by sampling recent messages."""
        try:
            samples: list[str] = []
            async for msg in self._client.iter_messages(telegram_id, limit=10):
                if msg.text:
                    samples.append(msg.text[:300])
                if len(samples) >= 5:
                    break
            await asyncio.sleep(_API_DELAY)

            if not samples:
                return False

            messages_text = "\n---\n".join(f"[{i+1}] {s}" for i, s in enumerate(samples))
            prompt = _CHANNEL_VALIDATION_PROMPT.format(messages=messages_text)

            result = await self._llm.chat_json(
                [{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=200,
            )
            if result and isinstance(result.get("is_relevant"), bool):
                if not result["is_relevant"]:
                    logger.debug("Channel %d rejected: %s", telegram_id, result.get("reason", ""))
                return result["is_relevant"]
        except FloodWaitError as e:
            logger.warning("FloodWait %ds during channel validation %d", e.seconds, telegram_id)
            await asyncio.sleep(e.seconds)
        except Exception:
            logger.exception("Channel validation failed for %d", telegram_id)
        # On error, reject to be safe — can retry next cycle
        return False

    def _at_capacity(self) -> bool:
        return len(self._known_ids) >= self._s.discovery_max_channels
