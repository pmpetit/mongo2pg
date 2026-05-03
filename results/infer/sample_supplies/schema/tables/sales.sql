CREATE TABLE sales (
    id UUID PRIMARY KEY,
    couponused BOOLEAN NOT NULL,
    purchasemethod TEXT NOT NULL,
    saledate TIMESTAMP WITH TIME ZONE NOT NULL,
    storelocation TEXT NOT NULL
);

CREATE TABLE sales_customer (
    id BIGSERIAL PRIMARY KEY,
    sales_id UUID NOT NULL,
    age INTEGER NOT NULL,
    email TEXT NOT NULL,
    gender TEXT NOT NULL,
    satisfaction INTEGER NOT NULL,
    FOREIGN KEY (sales_id) REFERENCES sales (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE sales_items (
    id BIGSERIAL PRIMARY KEY,
    sales_id UUID NOT NULL,
    name TEXT NOT NULL,
    price NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (sales_id) REFERENCES sales (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE sales_items_tags (
    id BIGSERIAL PRIMARY KEY,
    sales_items_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (sales_items_id) REFERENCES sales_items (id) DEFERRABLE INITIALLY DEFERRED
);