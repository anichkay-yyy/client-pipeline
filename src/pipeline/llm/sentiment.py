from __future__ import annotations

import logging
from typing import Any

from .openrouter import OpenRouterClient

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You analyze reply messages from potential clients to determine their sentiment.

Return ONLY valid JSON:
{
  "sentiment": "positive" | "negative" | "neutral" | "unclear",
  "wants_to_continue": true/false,
  "summary": "1-sentence summary of the reply"
}

Rules:
- "positive": client is interested, asks questions, wants to continue
- "negative": client declines, found someone else, not interested
- "neutral": generic acknowledgment without clear intent
- "unclear": cannot determine intent"""


async def analyze_sentiment(
    client: OpenRouterClient,
    our_message: str,
    their_reply: str,
) -> dict[str, Any] | None:
    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Our outreach message:\n{our_message}\n\n"
                f"Client's reply:\n{their_reply}"
            ),
        },
    ]
    return await client.chat_json(messages, temperature=0.2, max_tokens=200)
