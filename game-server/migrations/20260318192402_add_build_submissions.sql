CREATE TABLE build_submissions (
    submission_id TEXT PRIMARY KEY,
    upload_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    requested_by_subject TEXT NOT NULL,
    original_file_name TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL CHECK (file_size_bytes > 0),
    sha256_hex TEXT NOT NULL,
    staged_path TEXT NOT NULL,
    builder_build_id TEXT,
    cancellation_requested BOOLEAN NOT NULL DEFAULT FALSE,
    retry_of_submission_id TEXT REFERENCES build_submissions(submission_id),
    last_known_builder_status INTEGER,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    last_synced_at_ms BIGINT,
    CHECK (char_length(trim(submission_id)) > 0),
    CHECK (char_length(trim(upload_id)) > 0),
    CHECK (char_length(trim(team_id)) > 0),
    CHECK (char_length(trim(requested_by_subject)) > 0),
    CHECK (char_length(trim(original_file_name)) > 0),
    CHECK (char_length(trim(sha256_hex)) = 64),
    CHECK (char_length(trim(staged_path)) > 0),
    CHECK (builder_build_id IS NULL OR char_length(trim(builder_build_id)) > 0),
    CHECK (retry_of_submission_id IS NULL OR char_length(trim(retry_of_submission_id)) > 0),
    CHECK (updated_at_ms >= created_at_ms),
    CHECK (last_synced_at_ms IS NULL OR last_synced_at_ms >= created_at_ms)
);

CREATE UNIQUE INDEX build_submissions_team_upload_uidx
    ON build_submissions (team_id, upload_id);

CREATE UNIQUE INDEX build_submissions_builder_build_uidx
    ON build_submissions (builder_build_id)
    WHERE builder_build_id IS NOT NULL;

CREATE INDEX build_submissions_team_created_idx
    ON build_submissions (team_id, created_at_ms DESC);
