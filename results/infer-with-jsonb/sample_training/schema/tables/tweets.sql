CREATE TABLE tweets (
    id UUID PRIMARY KEY,
    coordinates JSONB,
    created_at TEXT NOT NULL,
    entities JSONB NOT NULL,
    favorited BOOLEAN NOT NULL,
    geo JSONB,
    field_id BIGINT NOT NULL,
    in_reply_to_screen_name TEXT,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id INTEGER,
    place JSONB,
    retweeted BOOLEAN NOT NULL,
    retweeted_status JSONB,
    source TEXT NOT NULL,
    text TEXT NOT NULL,
    truncated BOOLEAN NOT NULL,
    _user JSONB NOT NULL
);