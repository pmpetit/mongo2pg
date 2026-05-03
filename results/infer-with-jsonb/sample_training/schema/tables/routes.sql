CREATE TABLE routes (
    id UUID PRIMARY KEY,
    airline JSONB NOT NULL,
    airplane TEXT NOT NULL,
    codeshare VARCHAR(1) NOT NULL,
    dst_airport TEXT NOT NULL,
    src_airport TEXT NOT NULL,
    stops INTEGER NOT NULL
);