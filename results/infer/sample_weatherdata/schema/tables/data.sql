CREATE TABLE data (
    id UUID PRIMARY KEY,
    callletters TEXT NOT NULL,
    datasource INTEGER NOT NULL,
    elevation INTEGER NOT NULL,
    qualitycontrolprocess TEXT NOT NULL,
    st TEXT NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    type TEXT NOT NULL
);

CREATE TABLE data_airtemperature (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressurechange (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressureobservation (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_dewpoint (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
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

CREATE TABLE data_position (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_precipitationestimatedobservation (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    discrepancy INTEGER NOT NULL,
    estimatedwaterdepth INTEGER NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_presentweatherobservationmanual (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    condition INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_pressure (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_seasurfacetemperature (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_sections (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycondition (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    cavok TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycoverlayer (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_visibility (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wavemeasurement (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    method TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wind (
    id BIGSERIAL PRIMARY KEY,
    data_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (data_id) REFERENCES data (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressurechange_quantity24hours (
    id BIGSERIAL PRIMARY KEY,
    data_atmosphericpressurechange_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_atmosphericpressurechange_id) REFERENCES data_atmosphericpressurechange (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressurechange_quantity3hours (
    id BIGSERIAL PRIMARY KEY,
    data_atmosphericpressurechange_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_atmosphericpressurechange_id) REFERENCES data_atmosphericpressurechange (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressurechange_tendency (
    id BIGSERIAL PRIMARY KEY,
    data_atmosphericpressurechange_id BIGINT NOT NULL,
    code INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_atmosphericpressurechange_id) REFERENCES data_atmosphericpressurechange (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressureobservation_altimetersetting (
    id BIGSERIAL PRIMARY KEY,
    data_atmosphericpressureobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_atmosphericpressureobservation_id) REFERENCES data_atmosphericpressureobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_atmosphericpressureobservation_stationpressure (
    id BIGSERIAL PRIMARY KEY,
    data_atmosphericpressureobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_atmosphericpressureobservation_id) REFERENCES data_atmosphericpressureobservation (id) DEFERRABLE INITIALLY DEFERRED
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

CREATE TABLE data_position_coordinates (
    id BIGSERIAL PRIMARY KEY,
    data_position_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_position_id) REFERENCES data_position (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skycondition_ceilingheight (
    id BIGSERIAL PRIMARY KEY,
    data_skycondition_id BIGINT NOT NULL,
    determination TEXT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skycondition_id) REFERENCES data_skycondition (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_highcloudgenus (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_lowcloudgenus (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_lowestcloudbaseheight (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_lowestcloudcoverage (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_midcloudgenus (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_skyconditionobservation_totalcoverage (
    id BIGSERIAL PRIMARY KEY,
    data_skyconditionobservation_id BIGINT NOT NULL,
    opaque INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_skyconditionobservation_id) REFERENCES data_skyconditionobservation (id) DEFERRABLE INITIALLY DEFERRED
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

CREATE TABLE data_visibility_distance (
    id BIGSERIAL PRIMARY KEY,
    data_visibility_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (data_visibility_id) REFERENCES data_visibility (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_visibility_variability (
    id BIGSERIAL PRIMARY KEY,
    data_visibility_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (data_visibility_id) REFERENCES data_visibility (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wavemeasurement_seastate (
    id BIGSERIAL PRIMARY KEY,
    data_wavemeasurement_id BIGINT NOT NULL,
    code INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_wavemeasurement_id) REFERENCES data_wavemeasurement (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wavemeasurement_waves (
    id BIGSERIAL PRIMARY KEY,
    data_wavemeasurement_id BIGINT NOT NULL,
    height DOUBLE PRECISION NOT NULL,
    period INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_wavemeasurement_id) REFERENCES data_wavemeasurement (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wind_direction (
    id BIGSERIAL PRIMARY KEY,
    data_wind_id BIGINT NOT NULL,
    angle INTEGER NOT NULL,
    quality INTEGER NOT NULL,
    FOREIGN KEY (data_wind_id) REFERENCES data_wind (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE data_wind_speed (
    id BIGSERIAL PRIMARY KEY,
    data_wind_id BIGINT NOT NULL,
    quality INTEGER NOT NULL,
    rate DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (data_wind_id) REFERENCES data_wind (id) DEFERRABLE INITIALLY DEFERRED
);