CREATE TABLE tweets (
    id UUID PRIMARY KEY,
    created_at TEXT NOT NULL,
    favorited BOOLEAN NOT NULL,
    field_id BIGINT NOT NULL,
    in_reply_to_screen_name TEXT,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id INTEGER,
    retweeted BOOLEAN NOT NULL,
    source TEXT NOT NULL,
    text TEXT NOT NULL,
    truncated BOOLEAN NOT NULL
);

CREATE TABLE tweets_coordinates (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_geo (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_place (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    country TEXT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    full_name TEXT NOT NULL,
    field_id TEXT NOT NULL,
    name TEXT NOT NULL,
    place_type TEXT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    created_at TEXT NOT NULL,
    favorited BOOLEAN NOT NULL,
    field_id BIGINT NOT NULL,
    in_reply_to_screen_name TEXT,
    in_reply_to_status_id BIGINT,
    in_reply_to_user_id INTEGER,
    retweeted BOOLEAN NOT NULL,
    source TEXT NOT NULL,
    text TEXT NOT NULL,
    truncated BOOLEAN NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets__user (
    id BIGSERIAL PRIMARY KEY,
    tweets_id UUID NOT NULL,
    contributors_enabled BOOLEAN NOT NULL,
    created_at TEXT NOT NULL,
    description TEXT,
    favourites_count INTEGER NOT NULL,
    followers_count INTEGER NOT NULL,
    friends_count INTEGER NOT NULL,
    geo_enabled BOOLEAN NOT NULL,
    field_id INTEGER NOT NULL,
    lang TEXT NOT NULL,
    listed_count INTEGER NOT NULL,
    location TEXT,
    name TEXT NOT NULL,
    profile_background_color TEXT NOT NULL,
    profile_background_image_url TEXT NOT NULL,
    profile_background_tile BOOLEAN NOT NULL,
    profile_image_url TEXT NOT NULL,
    profile_link_color TEXT NOT NULL,
    profile_sidebar_border_color TEXT NOT NULL,
    profile_sidebar_fill_color TEXT NOT NULL,
    profile_text_color TEXT NOT NULL,
    profile_use_background_image BOOLEAN NOT NULL,
    protected BOOLEAN NOT NULL,
    screen_name TEXT NOT NULL,
    show_all_inline_media BOOLEAN NOT NULL,
    statuses_count INTEGER NOT NULL,
    time_zone TEXT,
    url TEXT,
    utc_offset INTEGER,
    verified BOOLEAN NOT NULL,
    FOREIGN KEY (tweets_id) REFERENCES tweets (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_coordinates_coordinates (
    id BIGSERIAL PRIMARY KEY,
    tweets_coordinates_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (tweets_coordinates_id) REFERENCES tweets_coordinates (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_hashtags (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    FOREIGN KEY (tweets_entities_id) REFERENCES tweets_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_urls (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_id BIGINT NOT NULL,
    expanded_url TEXT,
    url TEXT NOT NULL,
    FOREIGN KEY (tweets_entities_id) REFERENCES tweets_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_user_mentions (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_id BIGINT NOT NULL,
    field_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    screen_name TEXT NOT NULL,
    FOREIGN KEY (tweets_entities_id) REFERENCES tweets_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_geo_coordinates (
    id BIGSERIAL PRIMARY KEY,
    tweets_geo_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (tweets_geo_id) REFERENCES tweets_geo (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_place_attributes (
    id BIGSERIAL PRIMARY KEY,
    tweets_place_id BIGINT NOT NULL,
    FOREIGN KEY (tweets_place_id) REFERENCES tweets_place (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_place_bounding_box (
    id BIGSERIAL PRIMARY KEY,
    tweets_place_id BIGINT NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (tweets_place_id) REFERENCES tweets_place (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_id BIGINT NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_id) REFERENCES tweets_retweeted_status (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status__user (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_id BIGINT NOT NULL,
    contributors_enabled BOOLEAN NOT NULL,
    created_at TEXT NOT NULL,
    description TEXT,
    favourites_count INTEGER NOT NULL,
    followers_count INTEGER NOT NULL,
    friends_count INTEGER NOT NULL,
    geo_enabled BOOLEAN NOT NULL,
    field_id INTEGER NOT NULL,
    lang TEXT NOT NULL,
    listed_count INTEGER NOT NULL,
    location TEXT,
    name TEXT NOT NULL,
    profile_background_color TEXT NOT NULL,
    profile_background_image_url TEXT NOT NULL,
    profile_background_tile BOOLEAN NOT NULL,
    profile_image_url TEXT NOT NULL,
    profile_link_color TEXT NOT NULL,
    profile_sidebar_border_color TEXT NOT NULL,
    profile_sidebar_fill_color TEXT NOT NULL,
    profile_text_color TEXT NOT NULL,
    profile_use_background_image BOOLEAN NOT NULL,
    protected BOOLEAN NOT NULL,
    screen_name TEXT NOT NULL,
    show_all_inline_media BOOLEAN NOT NULL,
    statuses_count INTEGER NOT NULL,
    time_zone TEXT,
    url TEXT,
    utc_offset INTEGER,
    verified BOOLEAN NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_id) REFERENCES tweets_retweeted_status (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_hashtags_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_hashtags_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_entities_hashtags_id) REFERENCES tweets_entities_hashtags (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_urls_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_urls_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_entities_urls_id) REFERENCES tweets_entities_urls (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_entities_user_mentions_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_entities_user_mentions_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_entities_user_mentions_id) REFERENCES tweets_entities_user_mentions (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_place_bounding_box_coordinates (
    id BIGSERIAL PRIMARY KEY,
    tweets_place_bounding_box_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (tweets_place_bounding_box_id) REFERENCES tweets_place_bounding_box (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_hashtags (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_id) REFERENCES tweets_retweeted_status_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_urls (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_id BIGINT NOT NULL,
    url TEXT NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_id) REFERENCES tweets_retweeted_status_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_user_mentions (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_id BIGINT NOT NULL,
    field_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    screen_name TEXT NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_id) REFERENCES tweets_retweeted_status_entities (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_hashtags_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_hashtags_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_hashtags_id) REFERENCES tweets_retweeted_status_entities_hashtags (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_urls_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_urls_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_urls_id) REFERENCES tweets_retweeted_status_entities_urls (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE tweets_retweeted_status_entities_user_mentions_indices (
    id BIGSERIAL PRIMARY KEY,
    tweets_retweeted_status_entities_user_mentions_id BIGINT NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (tweets_retweeted_status_entities_user_mentions_id) REFERENCES tweets_retweeted_status_entities_user_mentions (id) DEFERRABLE INITIALLY DEFERRED
);