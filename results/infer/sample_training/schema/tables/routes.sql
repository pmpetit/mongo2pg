CREATE TABLE routes (
    id UUID PRIMARY KEY,
    airplane TEXT NOT NULL,
    codeshare VARCHAR(1) NOT NULL,
    dst_airport TEXT NOT NULL,
    src_airport TEXT NOT NULL,
    stops INTEGER NOT NULL
);

CREATE TABLE routes_airline (
    id BIGSERIAL PRIMARY KEY,
    routes_id UUID NOT NULL,
    alias TEXT NOT NULL,
    iata TEXT NOT NULL,
    field_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (routes_id) REFERENCES routes (id) DEFERRABLE INITIALLY DEFERRED
);