# Data contract — listening event v1

Atomic unit: one user interaction with one song at one instant.

Required fields:
`event_id`, `user_id`, `session_id`, `song_id`, `artist_id`, `genre`,
`event_type`, `event_ts`, `duration_ms`, `ms_played`, `_ingested_at`,
`_source_file`.

Invariants:
- `event_id` is globally unique.
- `event_type ∈ {play, skip, pause, replay, like}`.
- `30_000 <= duration_ms <= 900_000`.
- `0 <= ms_played <= duration_ms`.
- `skip => ms_played < 0.8 * duration_ms`.
- a `song_id` maps to a stable `artist_id` and `genre`.
- all events in one `session_id` belong to one `user_id`.
