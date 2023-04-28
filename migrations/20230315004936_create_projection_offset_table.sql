-- Add migration script here
CREATE TABLE IF NOT EXISTS projection_offset_store (
  projection_name VARCHAR(255) NOT NULL,
  projection_key VARCHAR(255) NOT NULL,
  current_offset VARCHAR(255) NOT NULL,
  manifest VARCHAR(4) NOT NULL,
  mergeable BOOLEAN NOT NULL,
  last_updated BIGINT NOT NULL,
  PRIMARY KEY(projection_name, projection_key)
);

CREATE INDEX IF NOT EXISTS coerce_projection_name_index ON coerce_projection_offset_store (projection_name);

--CREATE TABLE IF NOT EXISTS coerce_projection_management (
--  projection_name VARCHAR(255) NOT NULL,
--  projection_key VARCHAR(255) NOT NULL,
--  paused BOOLEAN NOT NULL,
--  last_updated BIGINT NOT NULL,
--  PRIMARY KEY(projection_name, projection_key)
--);