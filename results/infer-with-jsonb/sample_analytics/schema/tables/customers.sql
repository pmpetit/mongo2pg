CREATE TABLE customers (
    id UUID PRIMARY KEY,
    active BOOLEAN,
    address TEXT NOT NULL,
    birthdate TIMESTAMP WITH TIME ZONE NOT NULL,
    email TEXT NOT NULL,
    name TEXT NOT NULL,
    tier_and_details JSONB NOT NULL,
    username TEXT NOT NULL
);

CREATE TABLE customers_accounts (
    id BIGSERIAL PRIMARY KEY,
    customers_id UUID NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (customers_id) REFERENCES customers (id) DEFERRABLE INITIALLY DEFERRED
);