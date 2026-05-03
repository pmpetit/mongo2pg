CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    account_id INTEGER NOT NULL,
    _limit INTEGER NOT NULL
);

CREATE TABLE accounts_products (
    id BIGSERIAL PRIMARY KEY,
    accounts_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (accounts_id) REFERENCES accounts (id) DEFERRABLE INITIALLY DEFERRED
);