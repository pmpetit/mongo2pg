CREATE TABLE listingsandreviews (
    id BIGSERIAL PRIMARY KEY,
    access TEXT NOT NULL,
    accommodates INTEGER NOT NULL,
    bathrooms NUMERIC,
    bed_type TEXT NOT NULL,
    bedrooms INTEGER NOT NULL,
    beds INTEGER,
    calendar_last_scraped TIMESTAMP WITH TIME ZONE NOT NULL,
    cancellation_policy TEXT NOT NULL,
    cleaning_fee NUMERIC,
    description TEXT NOT NULL,
    extra_people NUMERIC NOT NULL,
    first_review TIMESTAMP WITH TIME ZONE,
    guests_included NUMERIC NOT NULL,
    house_rules TEXT NOT NULL,
    interaction TEXT NOT NULL,
    last_review TIMESTAMP WITH TIME ZONE,
    last_scraped TIMESTAMP WITH TIME ZONE NOT NULL,
    listing_url TEXT NOT NULL,
    maximum_nights INTEGER NOT NULL,
    minimum_nights INTEGER NOT NULL,
    monthly_price NUMERIC,
    name TEXT NOT NULL,
    neighborhood_overview TEXT NOT NULL,
    notes TEXT NOT NULL,
    number_of_reviews INTEGER NOT NULL,
    price NUMERIC NOT NULL,
    property_type TEXT NOT NULL,
    reviews_per_month INTEGER,
    room_type TEXT NOT NULL,
    security_deposit NUMERIC,
    space TEXT NOT NULL,
    summary TEXT NOT NULL,
    transit TEXT NOT NULL,
    weekly_price NUMERIC
);

CREATE TABLE listingsandreviews_address (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    country TEXT NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    government_area TEXT NOT NULL,
    market TEXT NOT NULL,
    street TEXT NOT NULL,
    suburb TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_amenities (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_availability (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    availability_30 INTEGER NOT NULL,
    availability_365 INTEGER NOT NULL,
    availability_60 INTEGER NOT NULL,
    availability_90 INTEGER NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_host (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    host_about TEXT NOT NULL,
    host_has_profile_pic BOOLEAN NOT NULL,
    host_id INTEGER NOT NULL,
    host_identity_verified BOOLEAN NOT NULL,
    host_is_superhost BOOLEAN NOT NULL,
    host_listings_count INTEGER NOT NULL,
    host_location TEXT NOT NULL,
    host_name TEXT NOT NULL,
    host_neighbourhood TEXT NOT NULL,
    host_picture_url TEXT NOT NULL,
    host_response_rate INTEGER,
    host_response_time TEXT,
    host_thumbnail_url TEXT NOT NULL,
    host_total_listings_count INTEGER NOT NULL,
    host_url TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_images (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    medium_url TEXT NOT NULL,
    picture_url TEXT NOT NULL,
    thumbnail_url TEXT NOT NULL,
    xl_picture_url TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_review_scores (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    review_scores_accuracy INTEGER,
    review_scores_checkin INTEGER,
    review_scores_cleanliness INTEGER,
    review_scores_communication INTEGER,
    review_scores_location INTEGER,
    review_scores_rating INTEGER,
    review_scores_value INTEGER,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_reviews (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_id BIGINT NOT NULL,
    comments TEXT,
    date TIMESTAMP WITH TIME ZONE NOT NULL,
    listing_id INTEGER NOT NULL,
    reviewer_id INTEGER NOT NULL,
    reviewer_name TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_id) REFERENCES listingsandreviews (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_address_location (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_address_id BIGINT NOT NULL,
    is_location_exact BOOLEAN NOT NULL,
    type TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_address_id) REFERENCES listingsandreviews_address (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_host_host_verifications (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_host_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (listingsandreviews_host_id) REFERENCES listingsandreviews_host (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE listingsandreviews_address_location_coordinates (
    id BIGSERIAL PRIMARY KEY,
    listingsandreviews_address_location_id BIGINT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (listingsandreviews_address_location_id) REFERENCES listingsandreviews_address_location (id) DEFERRABLE INITIALLY DEFERRED
);