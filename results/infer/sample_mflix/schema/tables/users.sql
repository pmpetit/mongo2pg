CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    password TEXT NOT NULL
);

CREATE TABLE users_preferences (
    id BIGSERIAL PRIMARY KEY,
    users_id UUID NOT NULL,
    FOREIGN KEY (users_id) REFERENCES users (id) DEFERRABLE INITIALLY DEFERRED
);