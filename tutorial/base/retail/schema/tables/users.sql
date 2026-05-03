CREATE TABLE users (
    id UUID PRIMARY KEY,
    address JSONB NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    surname TEXT NOT NULL,
    type TEXT NOT NULL,
    version INTEGER
);

CREATE TABLE users_lastrecommendations (
    id BIGSERIAL PRIMARY KEY,
    users_id UUID NOT NULL,
    brand TEXT NOT NULL,
    image TEXT NOT NULL,
    name TEXT NOT NULL,
    price INTEGER NOT NULL,
    productid TEXT NOT NULL,
    vectorsearchscore DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (users_id) REFERENCES users (id) DEFERRABLE INITIALLY DEFERRED
);