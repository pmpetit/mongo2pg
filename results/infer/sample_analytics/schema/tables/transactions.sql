CREATE TABLE transactions (
    id UUID PRIMARY KEY,
    account_id INTEGER NOT NULL,
    bucket_end_date TIMESTAMP WITH TIME ZONE NOT NULL,
    bucket_start_date TIMESTAMP WITH TIME ZONE NOT NULL,
    transaction_count INTEGER NOT NULL
);

CREATE TABLE transactions_transactions (
    id BIGSERIAL PRIMARY KEY,
    transactions_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    symbol TEXT NOT NULL,
    total DOUBLE PRECISION NOT NULL,
    transaction_code VARCHAR(4) NOT NULL,
    FOREIGN KEY (transactions_id) REFERENCES transactions (id) DEFERRABLE INITIALLY DEFERRED
);