CREATE TABLE products (
    id UUID PRIMARY KEY,
    articletype TEXT NOT NULL,
    basecolour TEXT,
    brand TEXT NOT NULL,
    code TEXT NOT NULL,
    description TEXT NOT NULL,
    gender TEXT NOT NULL,
    mastercategory TEXT,
    name TEXT NOT NULL,
    subcategory TEXT,
    year INTEGER
);

CREATE TABLE products_image (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE products_price (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    amount INTEGER NOT NULL,
    currency TEXT NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE products_vai_text_embedding (
    id BIGSERIAL PRIMARY KEY,
    products_id UUID NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (products_id) REFERENCES products (id) DEFERRABLE INITIALLY DEFERRED
);