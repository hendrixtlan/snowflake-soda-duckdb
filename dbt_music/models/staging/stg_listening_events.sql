{#-
  Deduplication happens here, but it is COUNTED, not hidden. A test that passes
  because the offending rows were silently removed upstream is worse than no
  test at all: it reports health that was manufactured by the pipeline.
  The discarded rows are surfaced by stg_rejected_events.
-#}
with src as (
    select * from {{ source('raw', 'listening_events') }}
),
ranked as (
    select
        event_id,
        user_id,
        session_id,
        song_id,
        artist_id,
        genre,
        event_type,
        event_ts,
        duration_ms,
        ms_played,
        device_type,
        country,
        _ingested_at,
        _source_file,
        _batch_id,
        case when duration_ms = 0 then null
             else ms_played / duration_ms::float end as completion_rate,
        row_number() over (partition by event_id order by _ingested_at desc) as rn,
        count(*)     over (partition by event_id)                            as event_id_occurrences
    from src
)
select * exclude (rn, event_id_occurrences),
       event_id_occurrences > 1 as was_deduplicated
from ranked
where rn = 1
