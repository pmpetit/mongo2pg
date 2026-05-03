CREATE TABLE orders (
    id UUID PRIMARY KEY,
    invoiceid TEXT NOT NULL,
    shipping_address TEXT NOT NULL,
    type TEXT NOT NULL,
    _user TEXT NOT NULL
);

CREATE TABLE orders_products (
    id BIGSERIAL PRIMARY KEY,
    orders_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    brand TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (orders_id) REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE orders_status_history (
    id BIGSERIAL PRIMARY KEY,
    orders_id UUID NOT NULL,
    status TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    FOREIGN KEY (orders_id) REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE orders_products_image (
    id BIGSERIAL PRIMARY KEY,
    orders_products_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (orders_products_id) REFERENCES orders_products (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE orders_products_price (
    id BIGSERIAL PRIMARY KEY,
    orders_products_id BIGINT NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT,
    FOREIGN KEY (orders_products_id) REFERENCES orders_products (id) DEFERRABLE INITIALLY DEFERRED
);