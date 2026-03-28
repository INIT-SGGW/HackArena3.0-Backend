CREATE TABLE team_selected_slots (
    team_id TEXT PRIMARY KEY,
    selected_slot_index SMALLINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (char_length(trim(team_id)) > 0),
    CHECK (selected_slot_index BETWEEN 1 AND 3)
);

