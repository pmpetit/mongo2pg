CREATE TABLE theaters (
    id UUID PRIMARY KEY,
    theaterid INTEGER NOT NULL
);

CREATE TABLE theaters_location (
    id BIGSERIAL PRIMARY KEY,
    theaters_id UUID NOT NULL,
    FOREIGN KEY (theaters_id) REFERENCES theaters (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE theaters_location_address (
    id BIGSERIAL PRIMARY KEY,
    theaters_location_id BIGINT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    street1 TEXT NOT NULL,
    street2 TEXT,
    zipcode INTEGER NOT NULL,
    FOREIGN KEY (theaters_location_id) REFERENCES theaters_location (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE theaters_location_geo (
    id BIGSERIAL PRIMARY KEY,
    theaters_location_id BIGINT NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (theaters_location_id) REFERENCES theaters_location (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE theaters_location_geo_coordinates (
    id BIGSERIAL PRIMARY KEY,
    theaters_location_geo_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (theaters_location_geo_id) REFERENCES theaters_location_geo (id) DEFERRABLE INITIALLY DEFERRED
);