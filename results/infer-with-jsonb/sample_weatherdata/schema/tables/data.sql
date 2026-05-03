CREATE TABLE data (
    id UUID PRIMARY KEY,
    airtemperature JSONB NOT NULL,
    atmosphericpressurechange JSONB,
    atmosphericpressureobservation JSONB,
    callletters TEXT NOT NULL,
    datasource INTEGER NOT NULL,
    dewpoint JSONB NOT NULL,
    elevation INTEGER NOT NULL,
    position JSONB,
    precipitationestimatedobservation JSONB NOT NULL,
    pressure JSONB NOT NULL,
    qualitycontrolprocess TEXT NOT NULL,
    seasurfacetemperature JSONB,
    skycondition JSONB NOT NULL,
    skyconditionobservation JSONB,
    st TEXT NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    type TEXT NOT NULL,
    visibility JSONB NOT NULL,
    wavemeasurement JSONB,
    wind JSONB NOT NULL
);

CREATE TABLE data_extremeairtemperature (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    code VARCHAR(1) NOT NULL,
    period DOUBLE PRECISION NOT NULL,
    quantity INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_liquidprecipitation (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    condition INTEGER NOT NULL,
    depth INTEGER NOT NULL,
    period INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_pastweatherobservationmanual (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_presentweatherobservationmanual (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    condition INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_sections (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycoverlayer (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_pastweatherobservationmanual_atmosphericcondition (
    id BIGSERIAL PRIMARY KEY,
    data_pastweatherobservationmanual_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_pastweatherobservationmanual_id) REFERENCES data_pastweatherobservationmanual (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_pastweatherobservationmanual_period (
    id BIGSERIAL PRIMARY KEY,
    data_pastweatherobservationmanual_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_pastweatherobservationmanual_id) REFERENCES data_pastweatherobservationmanual (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycoverlayer_baseheight (
    id BIGSERIAL PRIMARY KEY,
    data_skycoverlayer_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skycoverlayer_id) REFERENCES data_skycoverlayer (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycoverlayer_cloudtype (
    id BIGSERIAL PRIMARY KEY,
    data_skycoverlayer_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skycoverlayer_id) REFERENCES data_skycoverlayer (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycoverlayer_coverage (
    id BIGSERIAL PRIMARY KEY,
    data_skycoverlayer_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skycoverlayer_id) REFERENCES data_skycoverlayer (id) DEFERRABLE INITIALLY DEFERRED
);