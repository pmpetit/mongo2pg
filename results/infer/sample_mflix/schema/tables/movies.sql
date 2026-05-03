CREATE TABLE movies (
    id UUID PRIMARY KEY,
    fullplot TEXT,
    lastupdated TEXT NOT NULL,
    metacritic INTEGER,
    num_mflix_comments INTEGER,
    plot TEXT,
    poster TEXT,
    rated TEXT,
    released TIMESTAMP WITH TIME ZONE,
    runtime INTEGER,
    title TEXT NOT NULL,
    type TEXT NOT NULL,
    year TEXT NOT NULL
);

CREATE TABLE movies_awards (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    nominations INTEGER NOT NULL,
    text TEXT NOT NULL,
    wins INTEGER NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies__cast (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_countries (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_directors (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_genres (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_imdb (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    field_id INTEGER NOT NULL,
    rating TEXT NOT NULL,
    votes TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_languages (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_tomatoes (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    boxoffice TEXT,
    consensus TEXT,
    dvd TIMESTAMP WITH TIME ZONE,
    fresh INTEGER,
    lastupdated TIMESTAMP WITH TIME ZONE NOT NULL,
    production TEXT,
    rotten INTEGER,
    website TEXT,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_writers (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_tomatoes_critic (
    id BIGSERIAL PRIMARY KEY,
    movies_tomatoes_id BIGINT NOT NULL,
    meter INTEGER,
    numreviews INTEGER,
    rating DOUBLE PRECISION,
    FOREIGN KEY (movies_tomatoes_id) REFERENCES movies_tomatoes (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_tomatoes_viewer (
    id BIGSERIAL PRIMARY KEY,
    movies_tomatoes_id BIGINT NOT NULL,
    meter INTEGER,
    numreviews INTEGER NOT NULL,
    rating DOUBLE PRECISION,
    FOREIGN KEY (movies_tomatoes_id) REFERENCES movies_tomatoes (id) DEFERRABLE INITIALLY DEFERRED
);