CREATE TABLE build_uploads (
    upload_id TEXT PRIMARY KEY,
    team_id TEXT NOT NULL,
    requested_by_subject TEXT NOT NULL,
    original_file_name TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL CHECK (file_size_bytes > 0),
    sha256_hex TEXT NOT NULL,
    staged_path TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    CHECK (char_length(trim(upload_id)) > 0),
    CHECK (char_length(trim(team_id)) > 0),
    CHECK (char_length(trim(requested_by_subject)) > 0),
    CHECK (char_length(trim(original_file_name)) > 0),
    CHECK (char_length(trim(sha256_hex)) = 64),
    CHECK (char_length(trim(staged_path)) > 0)
);

CREATE INDEX build_uploads_team_created_idx
    ON build_uploads (team_id, created_at_ms DESC);
