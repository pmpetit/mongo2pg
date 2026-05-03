CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    password TEXT NOT NULL,
    preferences JSONB
);