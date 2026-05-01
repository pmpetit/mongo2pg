CREATE TABLE locations (
    id UUID PRIMARY KEY,
    city TEXT,
    country TEXT NOT NULL,
    cp TEXT NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    street_and_number TEXT NOT NULL,
    type TEXT NOT NULL
);