CREATE TABLE sessions (
    id UUID PRIMARY KEY,
    jwt TEXT NOT NULL,
    user_id TEXT NOT NULL
);