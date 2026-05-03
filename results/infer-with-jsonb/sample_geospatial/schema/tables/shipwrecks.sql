CREATE TABLE shipwrecks (
    id UUID PRIMARY KEY,
    chart TEXT NOT NULL,
    depth TEXT NOT NULL,
    feature_type TEXT NOT NULL,
    gp_quality TEXT NOT NULL,
    history TEXT NOT NULL,
    latdec DOUBLE PRECISION NOT NULL,
    londec DOUBLE PRECISION NOT NULL,
    quasou TEXT NOT NULL,
    recrd TEXT NOT NULL,
    sounding_type TEXT NOT NULL,
    vesslterms TEXT NOT NULL,
    watlev TEXT NOT NULL
);

CREATE TABLE shipwrecks_coordinates (
    id BIGSERIAL PRIMARY KEY,
    shipwrecks_id UUID NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (shipwrecks_id) REFERENCES shipwrecks (id) DEFERRABLE INITIALLY DEFERRED
);