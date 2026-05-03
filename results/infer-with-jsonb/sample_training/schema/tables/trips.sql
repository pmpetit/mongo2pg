CREATE TABLE trips (
    id UUID PRIMARY KEY,
    bikeid INTEGER NOT NULL,
    birth_year TEXT NOT NULL,
    end_station_id INTEGER NOT NULL,
    end_station_location JSONB NOT NULL,
    end_station_name TEXT NOT NULL,
    gender INTEGER NOT NULL,
    start_station_id INTEGER NOT NULL,
    start_station_location JSONB NOT NULL,
    start_station_name TEXT NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tripduration INTEGER NOT NULL,
    usertype TEXT NOT NULL
);