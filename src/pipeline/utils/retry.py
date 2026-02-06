from __future__ import annotations

import asyncio
import functools
import logging
import random
from typing import Any, Callable, TypeVar

from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def retry_on_flood(max_retries: int = 3) -> Callable[[F], F]:
    """Retry decorator that handles Telethon FloodWaitError with jitter."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except FloodWaitError as e:
                    jitter = random.uniform(1, 10)
                    wait = e.seconds + jitter
                    logger.warning(
                        "FloodWaitError: %ds (attempt %d/%d), sleeping %.1fs",
                        e.seconds, attempt, max_retries, wait,
                    )
                    if attempt == max_retries:
                        raise
                    await asyncio.sleep(wait)
            return None  # unreachable

        return wrapper  # type: ignore[return-value]

    return decorator
