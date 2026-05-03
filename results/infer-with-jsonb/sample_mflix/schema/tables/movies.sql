CREATE TABLE movies (
    id UUID PRIMARY KEY,
    awards JSONB NOT NULL,
    fullplot TEXT,
    imdb JSONB NOT NULL,
    lastupdated TEXT NOT NULL,
    metacritic INTEGER,
    num_mflix_comments INTEGER,
    plot TEXT,
    poster TEXT,
    rated TEXT,
    released TIMESTAMP WITH TIME ZONE,
    runtime INTEGER,
    title TEXT NOT NULL,
    tomatoes JSONB,
    type TEXT NOT NULL,
    year TEXT NOT NULL
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

CREATE TABLE movies_languages (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE movies_writers (
    id BIGSERIAL PRIMARY KEY,
    movies_id UUID NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (movies_id) REFERENCES movies (id) DEFERRABLE INITIALLY DEFERRED
);