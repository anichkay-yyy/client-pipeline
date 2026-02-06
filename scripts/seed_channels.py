"""Seed channels into the database from config.yaml.

Usage: python -m scripts.seed_channels
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline.config import Settings
from pipeline.db.models import Channel
from pipeline.db.repository import Repository


async def main() -> None:
    settings = Settings.load()
    repo = Repository(settings.database_path)
    await repo.connect()

    for seed in settings.channels_seed:
        username = seed.lstrip("@")
        ch = Channel(
            telegram_id=None,
            username=username,
            title=username,
            is_active=True,
        )
        # We can't resolve telegram_id without a running client,
        # so insert with username only. The main app will resolve on start.
        await repo.db.execute(
            """INSERT INTO channels (username, title, is_active)
               VALUES (?, ?, 1)
               ON CONFLICT DO NOTHING""",
            (username, username),
        )
        await repo.db.commit()
        print(f"Seeded channel: @{username}")

    channels = await repo.get_active_channels()
    print(f"\nTotal active channels: {len(channels)}")
    for ch in channels:
        print(f"  id={ch.id} username={ch.username} telegram_id={ch.telegram_id}")

    await repo.close()


if __name__ == "__main__":
    asyncio.run(main())
