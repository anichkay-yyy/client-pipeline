from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"


class OpenRouterClient:
    def __init__(self, api_key: str, default_model: str, fallback_model: str) -> None:
        self._api_key = api_key
        self._default_model = default_model
        self._fallback_model = fallback_model
        self._client = httpx.AsyncClient(
            timeout=60.0,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def chat(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 500,
        json_mode: bool = False,
    ) -> str:
        model = model or self._default_model
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}

        try:
            resp = await self._client.post(_BASE_URL, json=body)
            if resp.status_code in (429, 500, 502, 503):
                logger.warning(
                    "OpenRouter %s (model=%s), trying fallback", resp.status_code, model
                )
                return await self._fallback(messages, temperature, max_tokens, json_mode)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError:
            logger.warning("OpenRouter HTTP error (model=%s), trying fallback", model)
            return await self._fallback(messages, temperature, max_tokens, json_mode)

    async def _fallback(
        self,
        messages: list[dict[str, str]],
        temperature: float,
        max_tokens: int,
        json_mode: bool,
    ) -> str:
        body: dict[str, Any] = {
            "model": self._fallback_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}

        resp = await self._client.post(_BASE_URL, json=body)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    async def chat_json(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.7,
        max_tokens: int = 500,
    ) -> dict[str, Any] | None:
        raw = await self.chat(
            messages, model=model, temperature=temperature,
            max_tokens=max_tokens, json_mode=True,
        )
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("LLM returned invalid JSON: %s", raw[:200])
            return None
