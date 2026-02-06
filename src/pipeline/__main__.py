from __future__ import annotations

import asyncio
import logging
import signal
import sys

from .bot.notifier import Notifier
from .config import Settings
from .db.models import Channel
from .db.repository import Repository
from .llm.openrouter import OpenRouterClient
from .orchestrator import Orchestrator
from .telegram.client import create_userbot, start_userbot
from .telegram.collector import Collector, IncomingMessage
from .telegram.discovery import DiscoveryService
from .telegram.responder import Responder
from .telegram.sender import Sender

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("pipeline")


async def main() -> None:
    settings = Settings.load()

    # Database
    repo = Repository(settings.database_path)
    await repo.connect()

    # LLM
    llm = OpenRouterClient(
        api_key=settings.openrouter_api_key,
        default_model=settings.llm_model,
        fallback_model=settings.llm_fallback_model,
    )

    # Telegram userbot
    tg = create_userbot(
        session_name=settings.telegram_session_name,
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
    )
    await start_userbot(tg, settings.telegram_phone)

    # Load previously stored active channels
    chat_ids: list[int] = []
    for ch in await repo.get_active_channels():
        if ch.telegram_id:
            chat_ids.append(ch.telegram_id)

    # Resolve seed channels (if any remain in config)
    for seed in settings.channels_seed:
        try:
            entity = await tg.get_entity(seed)
            if entity.id not in chat_ids:
                await repo.upsert_channel(
                    Channel(
                        telegram_id=entity.id,
                        username=getattr(entity, "username", None),
                        title=getattr(entity, "title", seed),
                    )
                )
                chat_ids.append(entity.id)
                logger.info("Registered seed channel: %s (id=%d)", seed, entity.id)
        except Exception:
            logger.exception("Failed to resolve seed channel: %s", seed)

    # Collector
    queue: asyncio.Queue[IncomingMessage] = asyncio.Queue()
    collector = Collector(tg, queue)
    await collector.subscribe(chat_ids)

    # Discovery — run initial search before anything else
    discovery: DiscoveryService | None = None
    if settings.discovery_enabled:
        discovery = DiscoveryService(
            client=tg, repo=repo, collector=collector, settings=settings, llm=llm,
        )
        await discovery.init()
        initial = await discovery.search_by_keywords()
        logger.info("Initial discovery: found %d channels", initial)

    # Backfill all known channels (seed + discovered)
    for cid in list(collector._monitored_chats):
        await collector.backfill(cid, limit=settings.orchestrator_batch_size)

    # Sender & Responder
    sender = Sender(tg)
    responder = Responder(tg, repo, llm)
    await responder.init()

    # Notifier bot
    notifier = Notifier(settings.notify_bot_token, settings.notify_chat_id)

    # Orchestrator (create before start so we can register commands)
    orch = Orchestrator(
        settings=settings,
        repo=repo,
        llm=llm,
        collector=collector,
        sender=sender,
        responder=responder,
        notifier=notifier,
        queue=queue,
        discovery=discovery,
    )

    # Register bot commands for monitoring
    notifier.register_command("status", orch.get_status)
    notifier.register_command("channels", orch.get_channels_report)
    notifier.register_command("leads", orch.get_leads_report)
    await notifier.start()

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: orch.stop())

    try:
        await orch.run()
    finally:
        await notifier.stop()
        await llm.close()
        await repo.close()
        await tg.disconnect()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
