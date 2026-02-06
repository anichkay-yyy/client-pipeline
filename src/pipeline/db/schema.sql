CREATE TABLE IF NOT EXISTS channels (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id     INTEGER UNIQUE,
    username        TEXT,
    title           TEXT,
    is_active       INTEGER NOT NULL DEFAULT 1,
    discovered_from TEXT,
    discovered_at   TEXT
);

CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_msg_id INTEGER NOT NULL,
    channel_id      INTEGER NOT NULL REFERENCES channels(id),
    sender_id       INTEGER,
    sender_username TEXT,
    text            TEXT,
    date            TEXT,
    text_hash       TEXT,
    UNIQUE(channel_id, telegram_msg_id)
);

CREATE TABLE IF NOT EXISTS leads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id      INTEGER NOT NULL REFERENCES messages(id),
    status          TEXT NOT NULL DEFAULT 'new',
    relevance_score REAL,
    budget          TEXT,
    stack           TEXT,
    deadline        TEXT,
    language        TEXT,
    summary         TEXT,
    outreach_text   TEXT,
    dm_text         TEXT,
    outreach_msg_id INTEGER,
    dm_msg_id       INTEGER,
    contacted_at    TEXT,
    replied_at      TEXT,
    forwarded_at    TEXT
);

CREATE TABLE IF NOT EXISTS replies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL REFERENCES leads(id),
    telegram_msg_id INTEGER,
    sender_id       INTEGER,
    text            TEXT,
    sentiment       TEXT,
    received_at     TEXT
);

CREATE TABLE IF NOT EXISTS daily_budget (
    date            TEXT PRIMARY KEY,
    sends_used      INTEGER NOT NULL DEFAULT 0,
    max_sends       INTEGER NOT NULL DEFAULT 10
);

CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);
CREATE INDEX IF NOT EXISTS idx_messages_text_hash ON messages(text_hash);
CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
CREATE INDEX IF NOT EXISTS idx_leads_message ON leads(message_id);
CREATE INDEX IF NOT EXISTS idx_replies_lead ON replies(lead_id);
