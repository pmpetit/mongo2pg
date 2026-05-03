CREATE TABLE trips (
    id UUID PRIMARY KEY,
    bikeid INTEGER NOT NULL,
    birth_year TEXT NOT NULL,
    end_station_id INTEGER NOT NULL,
    end_station_name TEXT NOT NULL,
    gender INTEGER NOT NULL,
    start_station_id INTEGER NOT NULL,
    start_station_name TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tripduration INTEGER NOT NULL,
    usertype TEXT NOT NULL
);

CREATE TABLE trips_end_station_location (
    id BIGSERIAL PRIMARY KEY,
    trips_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (trips_id) REFERENCES trips (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE trips_start_station_location (
    id BIGSERIAL PRIMARY KEY,
    trips_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (trips_id) REFERENCES trips (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE trips_end_station_location_coordinates (
    id BIGSERIAL PRIMARY KEY,
    trips_end_station_location_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (trips_end_station_location_id) REFERENCES trips_end_station_location (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE trips_start_station_location_coordinates (
    id BIGSERIAL PRIMARY KEY,
    trips_start_station_location_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (trips_start_station_location_id) REFERENCES trips_start_station_location (id) DEFERRABLE INITIALLY DEFERRED
);