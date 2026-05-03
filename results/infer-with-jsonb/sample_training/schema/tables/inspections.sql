CREATE TABLE inspections (
    id UUID PRIMARY KEY,
    address JSONB NOT NULL,
    business_name TEXT NOT NULL,
    certificate_number TEXT NOT NULL,
    date TEXT NOT NULL,
    field_id TEXT NOT NULL,
    result TEXT NOT NULL,
    sector TEXT NOT NULL
);