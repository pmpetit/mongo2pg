CREATE TABLE stories (
    id UUID PRIMARY KEY,
    comments INTEGER NOT NULL,
    description TEXT NOT NULL,
    diggs INTEGER NOT NULL,
    href TEXT NOT NULL,
    field_id TEXT NOT NULL,
    link TEXT NOT NULL,
    media TEXT NOT NULL,
    promote_date INTEGER NOT NULL,
    status TEXT NOT NULL,
    submit_date INTEGER NOT NULL,
    title TEXT NOT NULL
);

CREATE TABLE stories_container (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE stories_shorturl (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    short_url TEXT NOT NULL,
    view_count INTEGER NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE stories_thumbnail (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    contenttype TEXT NOT NULL,
    height INTEGER NOT NULL,
    originalheight INTEGER NOT NULL,
    originalwidth INTEGER NOT NULL,
    src TEXT NOT NULL,
    width INTEGER NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE stories_topic (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    name TEXT NOT NULL,
    short_name TEXT NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE stories__user (
    id BIGSERIAL PRIMARY KEY,
    stories_id UUID NOT NULL,
    fullname TEXT,
    icon TEXT NOT NULL,
    name TEXT NOT NULL,
    profileviews INTEGER NOT NULL,
    registered INTEGER NOT NULL,
    FOREIGN KEY (stories_id) REFERENCES stories (id) DEFERRABLE INITIALLY DEFERRED
);