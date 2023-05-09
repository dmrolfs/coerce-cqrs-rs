-- Add migration script here
CREATE TABLE IF NOT EXISTS public.test_view(
  view_id TEXT NOT NULL,
  payload BYTEA NOT NULL,
  created_at BIGINT NOT NULL,
  last_updated_at BIGINT NOT NULL,

  PRIMARY KEY(view_id)
);