from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class RateLimiter:
    """Sliding-window rate limiter with jitter and warmup support."""

    def __init__(
        self,
        min_delay: int,
        max_delay: int,
        warmup_schedule: dict[str, int],
        started_at: datetime | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._warmup = warmup_schedule
        self._started_at = started_at or datetime.now(timezone.utc)
        self._last_send: float = 0

    def get_current_daily_limit(self) -> int:
        weeks = (datetime.now(timezone.utc) - self._started_at).days // 7 + 1
        if weeks <= 1:
            return self._warmup.get("week_1", 2)
        elif weeks <= 2:
            return self._warmup.get("week_2", 5)
        elif weeks <= 3:
            return self._warmup.get("week_3", 8)
        else:
            return self._warmup.get("week_4_plus", 12)

    async def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_send
        delay = random.uniform(self._min_delay, self._max_delay)
        remaining = delay - elapsed
        if remaining > 0:
            logger.debug("Rate limiter: sleeping %.1fs", remaining)
            await asyncio.sleep(remaining)
        self._last_send = time.monotonic()
