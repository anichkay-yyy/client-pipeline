from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Channel:
    id: int | None = None
    telegram_id: int | None = None
    username: str | None = None
    title: str | None = None
    is_active: bool = True
    discovered_from: str | None = None
    discovered_at: str | None = None


@dataclass
class Message:
    id: int | None = None
    telegram_msg_id: int = 0
    channel_id: int = 0
    sender_id: int | None = None
    sender_username: str | None = None
    text: str | None = None
    date: str | None = None
    text_hash: str | None = None


@dataclass
class Lead:
    id: int | None = None
    message_id: int = 0
    status: str = "new"
    relevance_score: float | None = None
    budget: str | None = None
    stack: str | None = None
    deadline: str | None = None
    language: str | None = None
    summary: str | None = None
    outreach_text: str | None = None
    dm_text: str | None = None
    outreach_msg_id: int | None = None
    dm_msg_id: int | None = None
    contacted_at: str | None = None
    replied_at: str | None = None
    forwarded_at: str | None = None


@dataclass
class Reply:
    id: int | None = None
    lead_id: int = 0
    telegram_msg_id: int | None = None
    sender_id: int | None = None
    text: str | None = None
    sentiment: str | None = None
    received_at: str | None = None


@dataclass
class DailyBudget:
    date: str = ""
    sends_used: int = 0
    max_sends: int = 10
