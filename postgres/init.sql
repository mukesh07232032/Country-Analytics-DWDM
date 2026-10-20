-- ==============================================================================
-- Country Analytics - PostgreSQL Database Initialization Schema
-- ==============================================================================
-- Idempotent initialization script for creating relational tables and indexes.
-- Designed for ingestion via Apache Airflow and reporting in Metabase.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 0. Airflow Metadata Database
-- Ensure dedicated metadata database exists for Apache Airflow on the same cluster,
-- isolating Airflow scheduler/dag tables from business analytics tables.
-- ------------------------------------------------------------------------------
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- ------------------------------------------------------------------------------
-- 1. Table: countries
-- Primary dimension and metric table storing core country demographics and geography.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS countries (
    country_code        VARCHAR(3) PRIMARY KEY,
    alpha2_code         VARCHAR(2),
    country_name        VARCHAR(150) NOT NULL,
    official_name       TEXT,
    capital             TEXT,
    region              VARCHAR(100),
    subregion           VARCHAR(100),
    population          BIGINT,
    area_km2            NUMERIC,
    latitude            NUMERIC,
    longitude           NUMERIC,
    population_density  NUMERIC,
    last_updated        TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Comments on table and attributes
COMMENT ON TABLE countries IS 'Core country information, geographical attributes, and demographic metrics.';
COMMENT ON COLUMN countries.country_code IS 'ISO 3166-1 alpha-3 three-letter unique country code (Primary Key).';
COMMENT ON COLUMN countries.alpha2_code IS 'ISO 3166-1 alpha-2 two-letter country code.';
COMMENT ON COLUMN countries.country_name IS 'Common name of the country.';
COMMENT ON COLUMN countries.official_name IS 'Official name of the country.';
COMMENT ON COLUMN countries.capital IS 'Primary capital city.';
COMMENT ON COLUMN countries.region IS 'Broad continental region (e.g., Africa, Americas, Asia, Europe, Oceania).';
COMMENT ON COLUMN countries.subregion IS 'Geographical sub-region (e.g., Northern Europe, Caribbean).';
COMMENT ON COLUMN countries.population IS 'Total population count.';
COMMENT ON COLUMN countries.area_km2 IS 'Total surface area in square kilometers.';
COMMENT ON COLUMN countries.latitude IS 'Geographic center latitude in decimal degrees.';
COMMENT ON COLUMN countries.longitude IS 'Geographic center longitude in decimal degrees.';
COMMENT ON COLUMN countries.population_density IS 'Calculated population density (population / area_km2). Handled safely during ETL when area is 0 or null.';
COMMENT ON COLUMN countries.last_updated IS 'Timestamp indicating when this record was ingested or updated by the pipeline.';

-- ------------------------------------------------------------------------------
-- 2. Table: country_languages
-- Normalized table handling the 1-to-many relationship between countries and spoken languages.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS country_languages (
    country_code   VARCHAR(3) NOT NULL,
    language_code  VARCHAR(30) NOT NULL,
    language_name  TEXT NOT NULL,
    native_name    TEXT,
    CONSTRAINT pk_country_languages PRIMARY KEY (country_code, language_code),
    CONSTRAINT fk_country_languages_country FOREIGN KEY (country_code)
        REFERENCES countries (country_code)
        ON DELETE CASCADE
);

COMMENT ON TABLE country_languages IS 'Spoken and official languages associated with each country.';
COMMENT ON COLUMN country_languages.country_code IS 'Foreign key referencing countries.country_code.';
COMMENT ON COLUMN country_languages.language_code IS 'Identifier code for language (e.g., BCP 47 or ISO 639 code).';
COMMENT ON COLUMN country_languages.language_name IS 'English name of the language.';
COMMENT ON COLUMN country_languages.native_name IS 'Native language name.';

-- ------------------------------------------------------------------------------
-- 3. Table: country_currencies
-- Normalized table handling the 1-to-many relationship between countries and currencies.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS country_currencies (
    country_code   VARCHAR(3) NOT NULL,
    currency_code  VARCHAR(10) NOT NULL,
    currency_name  TEXT,
    symbol         TEXT,
    CONSTRAINT pk_country_currencies PRIMARY KEY (country_code, currency_code),
    CONSTRAINT fk_country_currencies_country FOREIGN KEY (country_code)
        REFERENCES countries (country_code)
        ON DELETE CASCADE
);

COMMENT ON TABLE country_currencies IS 'Currencies officially recognized or used by each country.';
COMMENT ON COLUMN country_currencies.country_code IS 'Foreign key referencing countries.country_code.';
COMMENT ON COLUMN country_currencies.currency_code IS 'ISO 4217 three-letter currency code.';
COMMENT ON COLUMN country_currencies.currency_name IS 'Full currency name.';
COMMENT ON COLUMN country_currencies.symbol IS 'Currency symbol (e.g., $, €, £).';

-- ------------------------------------------------------------------------------
-- Performance Indexes
-- ------------------------------------------------------------------------------
-- Accelerates grouping, filtering, and aggregation by region and subregion
CREATE INDEX IF NOT EXISTS idx_countries_region ON countries (region);
CREATE INDEX IF NOT EXISTS idx_countries_subregion ON countries (subregion);

-- Accelerates range filtering and sorting for demographic and geographic queries in Metabase
CREATE INDEX IF NOT EXISTS idx_countries_population ON countries (population);
CREATE INDEX IF NOT EXISTS idx_countries_area_km2 ON countries (area_km2);
CREATE INDEX IF NOT EXISTS idx_countries_population_density ON countries (population_density);
