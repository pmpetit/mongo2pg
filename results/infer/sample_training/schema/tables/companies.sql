CREATE TABLE companies (
    id UUID PRIMARY KEY,
    alias_list TEXT,
    blog_feed_url TEXT,
    blog_url TEXT,
    category_code TEXT,
    created_at TEXT NOT NULL,
    crunchbase_url TEXT NOT NULL,
    deadpooled_day INTEGER,
    deadpooled_month INTEGER,
    deadpooled_url TEXT,
    deadpooled_year INTEGER,
    description TEXT,
    email_address TEXT,
    founded_day INTEGER,
    founded_month INTEGER,
    founded_year INTEGER,
    homepage_url TEXT,
    name TEXT NOT NULL,
    number_of_employees INTEGER,
    overview TEXT,
    permalink TEXT NOT NULL,
    phone_number TEXT,
    tag_list TEXT,
    total_money_raised TEXT NOT NULL,
    twitter_username TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE companies_acquisition (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    acquired_day INTEGER,
    acquired_month INTEGER,
    acquired_year INTEGER,
    price_amount BIGINT,
    price_currency_code VARCHAR(3) NOT NULL,
    source_description TEXT NOT NULL,
    source_url TEXT NOT NULL,
    term_code TEXT,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_acquisitions (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    acquired_day INTEGER,
    acquired_month INTEGER,
    acquired_year INTEGER,
    price_amount BIGINT,
    price_currency_code VARCHAR(3) NOT NULL,
    source_description TEXT,
    source_url TEXT,
    term_code TEXT,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_competitions (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_external_links (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    external_url TEXT NOT NULL,
    title TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_funding_rounds (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    funded_day INTEGER,
    funded_month INTEGER,
    funded_year INTEGER,
    field_id INTEGER NOT NULL,
    raised_amount INTEGER,
    raised_currency_code VARCHAR(3),
    round_code TEXT NOT NULL,
    source_description TEXT,
    source_url TEXT,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_image (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_investments (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_ipo (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    pub_day INTEGER,
    pub_month INTEGER,
    pub_year INTEGER,
    stock_symbol TEXT NOT NULL,
    valuation_amount BIGINT,
    valuation_currency_code VARCHAR(3) NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_milestones (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    description TEXT NOT NULL,
    field_id INTEGER NOT NULL,
    source_description TEXT NOT NULL,
    source_text TEXT,
    source_url TEXT NOT NULL,
    stoneable_type TEXT NOT NULL,
    stoned_day INTEGER,
    stoned_month INTEGER,
    stoned_year INTEGER NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_offices (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    address1 TEXT,
    address2 TEXT,
    city TEXT,
    country_code VARCHAR(3) NOT NULL,
    description TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    state_code VARCHAR(2),
    zip_code TEXT,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_partners (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    homepage_url TEXT NOT NULL,
    link_1_name TEXT NOT NULL,
    link_1_url TEXT NOT NULL,
    link_2_name TEXT,
    link_2_url TEXT,
    partner_name TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_products (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_providerships (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    is_past BOOLEAN,
    title TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_relationships (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    is_past BOOLEAN,
    title TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_screenshots (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_video_embeds (
    id BIGSERIAL PRIMARY KEY,
    companies_id UUID NOT NULL,
    description TEXT NOT NULL,
    embed_code TEXT NOT NULL,
    FOREIGN KEY (companies_id) REFERENCES companies (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_acquisition_acquiring_company (
    id BIGSERIAL PRIMARY KEY,
    companies_acquisition_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_acquisition_id) REFERENCES companies_acquisition (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_acquisitions_company (
    id BIGSERIAL PRIMARY KEY,
    companies_acquisitions_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_acquisitions_id) REFERENCES companies_acquisitions (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_competitions_competitor (
    id BIGSERIAL PRIMARY KEY,
    companies_competitions_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_competitions_id) REFERENCES companies_competitions (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_funding_rounds_investments (
    id BIGSERIAL PRIMARY KEY,
    companies_funding_rounds_id BIGINT NOT NULL,
    FOREIGN KEY (companies_funding_rounds_id) REFERENCES companies_funding_rounds (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_image_available_sizes (
    id BIGSERIAL PRIMARY KEY,
    companies_image_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (companies_image_id) REFERENCES companies_image (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_investments_funding_round (
    id BIGSERIAL PRIMARY KEY,
    companies_investments_id BIGINT NOT NULL,
    funded_day INTEGER,
    funded_month INTEGER,
    funded_year INTEGER NOT NULL,
    raised_amount INTEGER,
    raised_currency_code VARCHAR(3),
    round_code TEXT NOT NULL,
    source_description TEXT NOT NULL,
    source_url TEXT NOT NULL,
    FOREIGN KEY (companies_investments_id) REFERENCES companies_investments (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_milestones_stoneable (
    id BIGSERIAL PRIMARY KEY,
    companies_milestones_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_milestones_id) REFERENCES companies_milestones (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_providerships_provider (
    id BIGSERIAL PRIMARY KEY,
    companies_providerships_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_providerships_id) REFERENCES companies_providerships (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_relationships_person (
    id BIGSERIAL PRIMARY KEY,
    companies_relationships_id BIGINT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_relationships_id) REFERENCES companies_relationships (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_screenshots_available_sizes (
    id BIGSERIAL PRIMARY KEY,
    companies_screenshots_id BIGINT NOT NULL,
    value TEXT NOT NULL,
    FOREIGN KEY (companies_screenshots_id) REFERENCES companies_screenshots (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_funding_rounds_investments_company (
    id BIGSERIAL PRIMARY KEY,
    companies_funding_rounds_investments_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_funding_rounds_investments_id) REFERENCES companies_funding_rounds_investments (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_funding_rounds_investments_financial_org (
    id BIGSERIAL PRIMARY KEY,
    companies_funding_rounds_investments_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_funding_rounds_investments_id) REFERENCES companies_funding_rounds_investments (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_funding_rounds_investments_person (
    id BIGSERIAL PRIMARY KEY,
    companies_funding_rounds_investments_id BIGINT NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_funding_rounds_investments_id) REFERENCES companies_funding_rounds_investments (id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE companies_investments_funding_round_company (
    id BIGSERIAL PRIMARY KEY,
    companies_investments_funding_round_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    permalink TEXT NOT NULL,
    FOREIGN KEY (companies_investments_funding_round_id) REFERENCES companies_investments_funding_round (id) DEFERRABLE INITIALLY DEFERRED
);