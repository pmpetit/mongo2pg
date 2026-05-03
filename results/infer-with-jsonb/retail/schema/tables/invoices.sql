CREATE TABLE invoices (
    id UUID PRIMARY KEY,
    createdat TEXT NOT NULL,
    metadata JSONB NOT NULL,
    orderid TEXT NOT NULL,
    status TEXT NOT NULL,
    totalamount INTEGER NOT NULL,
    userid TEXT NOT NULL
);

CREATE TABLE invoices_items (
    id BIGSERIAL PRIMARY KEY,
    invoices_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    brand TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (invoices_id) REFERENCES invoices (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE invoices_recommendations (
    id BIGSERIAL PRIMARY KEY,
    invoices_id UUID NOT NULL,
    brand TEXT NOT NULL,
    image TEXT NOT NULL,
    name TEXT NOT NULL,
    price INTEGER NOT NULL,
    productid TEXT NOT NULL,
    vectorsearchscore DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (invoices_id) REFERENCES invoices (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE invoices_items_image (
    id BIGSERIAL PRIMARY KEY,
    invoices_items_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (invoices_items_id) REFERENCES invoices_items (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE invoices_items_price (
    id BIGSERIAL PRIMARY KEY,
    invoices_items_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT,
    FOREIGN KEY (invoices_items_id) REFERENCES invoices_items (id) DEFERRABLE INITIALLY DEFERRED
);