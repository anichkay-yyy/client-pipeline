from __future__ import annotations

import logging
from typing import Any

from .openrouter import OpenRouterClient

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an order classifier for a freelance automation pipeline.

Your job: determine if the given message is a genuine client ORDER (someone looking to hire a developer), and extract structured fields.

Rules:
- Ads, self-promotion, "looking for a job/work" posts are NOT orders.
- Detect the LANGUAGE of the message and always return it in the `language` field.
- `relevance_score` (0.0-1.0): how relevant this order is to web development, Telegram bots, and automation.
- If you cannot determine a field, set it to null.

Return ONLY valid JSON:
{
  "is_order": true/false,
  "relevance_score": 0.0-1.0,
  "budget": "extracted budget or null",
  "stack": "technologies mentioned or null",
  "deadline": "deadline mentioned or null",
  "language": "ru/en/uk/...",
  "summary": "1-2 sentence summary of the order"
}"""


async def classify_message(
    client: OpenRouterClient,
    text: str,
    target_stacks: list[str],
) -> dict[str, Any] | None:
    stacks_str = ", ".join(target_stacks)
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Target stacks: {stacks_str}\n\n"
                f"Message:\n{text}"
            ),
        },
    ]
    result = await client.chat_json(
        messages, temperature=0.1, max_tokens=300
    )
    if result is None:
        logger.warning("Classifier returned None for message: %s", text[:80])
        return None

    if not result.get("is_order"):
        logger.debug("Not an order: %s", text[:60])
        return None

    return result
