CREATE TABLE inspections (
    id UUID PRIMARY KEY,
    business_name TEXT NOT NULL,
    certificate_number TEXT NOT NULL,
    date TEXT NOT NULL,
    field_id TEXT NOT NULL,
    result TEXT NOT NULL,
    sector TEXT NOT NULL
);

CREATE TABLE inspections_address (
    id BIGSERIAL PRIMARY KEY,
    inspections_id UUID NOT NULL,
    city TEXT NOT NULL,
    number TEXT NOT NULL,
    street TEXT NOT NULL,
    zip INTEGER NOT NULL,
    FOREIGN KEY (inspections_id) REFERENCES inspections (id) DEFERRABLE INITIALLY DEFERRED
);