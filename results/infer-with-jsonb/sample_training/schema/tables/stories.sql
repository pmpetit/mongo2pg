CREATE TABLE stories (
    id UUID PRIMARY KEY,
    comments INTEGER NOT NULL,
    container JSONB NOT NULL,
    description TEXT NOT NULL,
    diggs INTEGER NOT NULL,
    href TEXT NOT NULL,
    field_id TEXT NOT NULL,
    inaccurate INTEGER,
    link TEXT NOT NULL,
    media TEXT NOT NULL,
    promote_date INTEGER NOT NULL,
    status TEXT NOT NULL,
    submit_date INTEGER NOT NULL,
    takedowndays INTEGER,
    takedownuri TEXT,
    thumbnail JSONB,
    title TEXT NOT NULL,
    topic JSONB NOT NULL,
    _user JSONB NOT NULL
);

CREATE TABLE stories_shorturl (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    short_url TEXT NOT NULL,
    view_count INTEGER NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);