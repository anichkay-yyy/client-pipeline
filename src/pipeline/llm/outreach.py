from __future__ import annotations

import logging

from .openrouter import OpenRouterClient

logger = logging.getLogger(__name__)

_THREAD_SYSTEM = """\
You are a freelance developer writing a short reply to a potential client's order in a Telegram channel thread.

Rules:
- Reply in the SAME LANGUAGE as the client's message (language: {language}).
- 3-5 sentences max.
- Do NOT mention price or give estimates.
- Briefly mention relevant experience (web development, Telegram bots, automation).
- End with ONE clarifying question about the project.
- Be friendly and professional, not salesy.
- Each reply must be unique — no templates."""

_DM_SYSTEM = """\
You are a freelance developer writing a short DM to a potential client who posted an order in a Telegram channel.

Rules:
- Reply in the SAME LANGUAGE as the client's message (language: {language}).
- 2-3 sentences max.
- Start by mentioning you saw their post in the channel about [topic].
- Do NOT mention price.
- Be concise and friendly."""


async def generate_thread_reply(
    client: OpenRouterClient,
    order_text: str,
    language: str,
    *,
    temperature: float = 0.7,
    max_tokens: int = 500,
) -> str:
    messages = [
        {"role": "system", "content": _THREAD_SYSTEM.format(language=language)},
        {"role": "user", "content": f"Client's order:\n{order_text}"},
    ]
    return await client.chat(messages, temperature=temperature, max_tokens=max_tokens)


async def generate_dm(
    client: OpenRouterClient,
    order_text: str,
    language: str,
    channel_title: str,
    *,
    temperature: float = 0.7,
    max_tokens: int = 300,
) -> str:
    messages = [
        {"role": "system", "content": _DM_SYSTEM.format(language=language)},
        {
            "role": "user",
            "content": (
                f"Channel: {channel_title}\n"
                f"Client's order:\n{order_text}"
            ),
        },
    ]
    return await client.chat(messages, temperature=temperature, max_tokens=max_tokens)
