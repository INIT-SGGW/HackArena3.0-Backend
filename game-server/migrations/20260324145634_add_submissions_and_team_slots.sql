CREATE TYPE submission_status AS ENUM (
    'queued',
    'building',
    'succeeded',
    'failed'
);

CREATE TABLE submissions (
    submission_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    description TEXT,
    wrapper_kind TEXT NOT NULL,
    wrapper_version TEXT NOT NULL,
    status submission_status NOT NULL,
    archive_path TEXT NOT NULL,
    image_ref TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (char_length(trim(submission_id)) > 0),
    CHECK (char_length(trim(team_id)) > 0),
    CHECK (char_length(trim(user_id)) > 0),
    CHECK (char_length(trim(wrapper_kind)) > 0),
    CHECK (char_length(trim(wrapper_version)) > 0),
    CHECK (char_length(trim(archive_path)) > 0)
);

CREATE INDEX submissions_team_id_created_at_idx
    ON submissions (team_id, created_at DESC);

CREATE INDEX submissions_status_created_at_idx
    ON submissions (status, created_at ASC);

CREATE TABLE team_submission_slots (
    team_id TEXT NOT NULL,
    slot_index SMALLINT NOT NULL,
    submission_id TEXT REFERENCES submissions (submission_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (team_id, slot_index),
    CHECK (char_length(trim(team_id)) > 0),
    CHECK (slot_index BETWEEN 1 AND 3)
);

CREATE INDEX team_submission_slots_submission_id_idx
    ON team_submission_slots (submission_id);
