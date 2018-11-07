/*
 * Creates the schema required for BG-Counter Tools.
 * Written for PostgreSQL 9.5.
 */

CREATE DOMAIN latitude AS NUMERIC
    CONSTRAINT valid_latitude CHECK (@ value <= 90);

CREATE DOMAIN longitude AS NUMERIC
    CONSTRAINT valid_longitude CHECK (@ value <= 180);

CREATE TABLE providers (
    prefix TEXT PRIMARY KEY,
    api_key TEXT UNIQUE CONSTRAINT valid_api_key CHECK (api_key ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
    org_name TEXT,
    org_email TEXT,
    org_url TEXT,
    contact_first_name TEXT,
    contact_last_name TEXT,
    contact_email TEXT,
    last_download TIMESTAMP,
    study_tag TEXT UNIQUE NOT NULL,
    study_tag_number TEXT UNIQUE NOT NULL,
    obfuscate BOOLEAN NOT NULL,
    CONSTRAINT name_exists CHECK ((org_name IS NOT NULL) OR (contact_first_name IS NOT NULL) OR (contact_last_name IS NOT NULL)),
    CONSTRAINT email_exists CHECK ((org_email IS NOT NULL) OR (contact_email IS NOT NULL))
);

CREATE TABLE ordinals (
    prefix TEXT NOT NULL REFERENCES providers ON UPDATE CASCADE,
    year INTEGER NOT NULL CONSTRAINT valid_year CHECK (year >= 1900),
    ordinal INTEGER DEFAULT 0 NOT NULL CONSTRAINT valid_ordinal CHECK (ordinal >= 0),

    PRIMARY KEY (prefix, year)
);

CREATE TABLE traps (
    trap_id TEXT PRIMARY KEY CONSTRAINT valid_trap_id CHECK (trap_id ~ '^[0-9]{15}$'),
    prefix TEXT NOT NULL REFERENCES providers ON UPDATE CASCADE
);

CREATE TABLE locations (
    trap_id TEXT NOT NULL REFERENCES traps,
    true_latitude latitude NOT NULL,
    true_longitude longitude NOT NULL,
    offset_latitude latitude NOT NULL,
    offset_longitude longitude NOT NULL,
    PRIMARY KEY (trap_id, true_latitude, true_longitude)
);
