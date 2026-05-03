CREATE TABLE customers (
    id UUID PRIMARY KEY,
    active BOOLEAN,
    address TEXT NOT NULL,
    birthdate TIMESTAMP WITH TIME ZONE NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    username TEXT NOT NULL
);

CREATE TABLE customers_accounts (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE customers_tier_and_details (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    key TEXT NOT NULL,
    active BOOLEAN NOT NULL,
    field_id TEXT NOT NULL,
    tier TEXT NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE customers_tier_and_details_benefits (
    id BIGSERIAL PRIMARY KEY,
    customers_tier_and_details_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (customers_tier_and_details_id) REFERENCES customers_tier_and_details (id) DEFERRABLE INITIALLY DEFERRED
);