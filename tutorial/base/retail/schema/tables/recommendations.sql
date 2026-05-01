CREATE TABLE recommendations (
    id UUID PRIMARY KEY,
    createdat TIMESTAMP WITH TIME ZONE NOT NULL,
    invoiceid TEXT NOT NULL,
    userid TEXT NOT NULL
);

CREATE TABLE recommendations_items (
    id BIGSERIAL PRIMARY KEY,
    recommendations_id UUID NOT NULL,
    brand TEXT NOT NULL,
    name TEXT NOT NULL,
    productid TEXT NOT NULL,
    vectorsearchscore DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (recommendations_id) REFERENCES recommendations (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE recommendations_items_image (
    id BIGSERIAL PRIMARY KEY,
    recommendations_items_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (recommendations_items_id) REFERENCES recommendations_items (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE recommendations_items_price (
    id BIGSERIAL PRIMARY KEY,
    recommendations_items_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    FOREIGN KEY (recommendations_items_id) REFERENCES recommendations_items (id) DEFERRABLE INITIALLY DEFERRED
);