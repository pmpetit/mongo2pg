CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    surname TEXT NOT NULL,
    type TEXT NOT NULL,
    version INTEGER
);

CREATE TABLE users_address (
    id BIGSERIAL PRIMARY KEY,
    users_id UUID NOT NULL,
    city TEXT NOT NULL,
    country TEXT NOT NULL,
    cp TEXT NOT NULL,
    state TEXT NOT NULL,
    street_and_number TEXT NOT NULL,
    FOREIGN KEY (users_id) REFERENCES users (id)
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
    FOREIGN KEY (users_id) REFERENCES users (id)
);