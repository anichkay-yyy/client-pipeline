from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


class Settings(BaseSettings):
    # Telegram
    telegram_api_id: int = 0
    telegram_api_hash: str = ""
    telegram_phone: str = ""
    telegram_session_name: str = "pipeline_session"

    # Notify bot
    notify_bot_token: str = ""
    notify_chat_id: int = 0

    # OpenRouter
    openrouter_api_key: str = ""

    # DB
    database_path: str = "data/pipeline.db"

    # YAML-loaded sections (populated in load())
    channels_seed: list[str] = Field(default_factory=list)

    rate_max_sends_per_day: int = 10
    rate_min_delay: int = 120
    rate_max_delay: int = 600
    rate_warmup_week_1: int = 2
    rate_warmup_week_2: int = 5
    rate_warmup_week_3: int = 8
    rate_warmup_week_4_plus: int = 12

    classification_min_relevance: float = 0.6
    classification_target_stacks: list[str] = Field(default_factory=list)

    llm_model: str = "anthropic/claude-sonnet-4"
    llm_fallback_model: str = "openai/gpt-4o"
    llm_temperature: float = 0.7
    llm_max_tokens: int = 500

    orchestrator_batch_size: int = 50
    orchestrator_cycle_interval: int = 30

    response_check_interval: int = 60
    response_no_reply_ttl_hours: int = 72

    # Discovery
    discovery_enabled: bool = False
    discovery_interval: int = 3600
    discovery_search_keywords: list[str] = Field(default_factory=list)
    discovery_keyword_languages: list[str] = Field(default_factory=lambda: ["ru", "en"])
    discovery_keywords_per_language: int = 10
    discovery_search_limit: int = 20
    discovery_max_channels: int = 100
    discovery_backfill_on_discover: int = 50
    discovery_scan_forwards: bool = True
    discovery_scan_descriptions: bool = True

    model_config = {"env_file": ".env", "extra": "ignore"}

    @classmethod
    def load(cls, env_file: str | Path = ".env", yaml_file: str | Path = "config.yaml") -> Settings:
        env_path = _PROJECT_ROOT / env_file
        yaml_path = _PROJECT_ROOT / yaml_file

        kwargs: dict[str, Any] = {}
        if env_path.exists():
            kwargs["_env_file"] = str(env_path)

        if yaml_path.exists():
            cfg = _load_yaml(yaml_path)
            channels = cfg.get("channels", {})
            kwargs["channels_seed"] = channels.get("seed", [])

            rl = cfg.get("rate_limiting", {})
            kwargs["rate_max_sends_per_day"] = rl.get("max_sends_per_day", 10)
            kwargs["rate_min_delay"] = rl.get("min_delay_seconds", 120)
            kwargs["rate_max_delay"] = rl.get("max_delay_seconds", 600)
            warmup = rl.get("warmup", {})
            kwargs["rate_warmup_week_1"] = warmup.get("week_1", 2)
            kwargs["rate_warmup_week_2"] = warmup.get("week_2", 5)
            kwargs["rate_warmup_week_3"] = warmup.get("week_3", 8)
            kwargs["rate_warmup_week_4_plus"] = warmup.get("week_4_plus", 12)

            cl = cfg.get("classification", {})
            kwargs["classification_min_relevance"] = cl.get("min_relevance_score", 0.6)
            kwargs["classification_target_stacks"] = cl.get("target_stacks", [])

            llm = cfg.get("llm", {})
            kwargs["llm_model"] = llm.get("model", "anthropic/claude-sonnet-4")
            kwargs["llm_fallback_model"] = llm.get("fallback_model", "openai/gpt-4o")
            kwargs["llm_temperature"] = llm.get("temperature", 0.7)
            kwargs["llm_max_tokens"] = llm.get("max_tokens", 500)

            orch = cfg.get("orchestrator", {})
            kwargs["orchestrator_batch_size"] = orch.get("collect_batch_size", 50)
            kwargs["orchestrator_cycle_interval"] = orch.get("cycle_interval_seconds", 30)

            rh = cfg.get("response_handler", {})
            kwargs["response_check_interval"] = rh.get("check_interval_seconds", 60)
            kwargs["response_no_reply_ttl_hours"] = rh.get("no_reply_ttl_hours", 72)

            disc = cfg.get("discovery", {})
            kwargs["discovery_enabled"] = disc.get("enabled", False)
            kwargs["discovery_interval"] = disc.get("interval_seconds", 3600)
            kwargs["discovery_search_keywords"] = disc.get("search_keywords", [])
            kwargs["discovery_keyword_languages"] = disc.get("keyword_languages", ["ru", "en"])
            kwargs["discovery_keywords_per_language"] = disc.get("keywords_per_language", 10)
            kwargs["discovery_search_limit"] = disc.get("search_limit", 20)
            kwargs["discovery_max_channels"] = disc.get("max_channels", 100)
            kwargs["discovery_backfill_on_discover"] = disc.get("backfill_on_discover", 50)
            kwargs["discovery_scan_forwards"] = disc.get("scan_forwards", True)
            kwargs["discovery_scan_descriptions"] = disc.get("scan_descriptions", True)

        settings = cls(**kwargs)
        logger.info("Settings loaded (db=%s)", settings.database_path)
        return settings
