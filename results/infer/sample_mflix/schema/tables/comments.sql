CREATE TABLE comments (
    id UUID PRIMARY KEY,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    email TEXT NOT NULL,
    movie_id TEXT NOT NULL,
    name TEXT NOT NULL,
    text TEXT NOT NULL
);