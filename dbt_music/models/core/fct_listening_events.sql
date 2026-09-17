{#-
  LEFT JOIN, deliberately. An inner join here would drop events whose song_id is
  not in the catalogue, and the relationships test downstream would then pass
  because the evidence was destroyed. Orphans come through flagged, the test
  fires, and stg_rejected_events records why.
-#}
{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
  )
}}

select
    e.event_id,
    e.user_id,
    e.session_id,
    e.song_id,
    e.artist_id,
    e.genre,
    e.event_type,
    e.event_ts,
    e.duration_ms,
    e.ms_played,
    e.completion_rate,
    e.device_type,
    e.country,
    e._ingested_at,
    e._batch_id,
    e.was_deduplicated,
    s.song_id is null as is_orphan_song,
    a.artist_id is null as is_orphan_artist
from {{ ref('stg_listening_events') }} e
left join {{ ref('stg_songs') }}   s on e.song_id   = s.song_id
left join {{ ref('stg_artists') }} a on e.artist_id = a.artist_id

{% if is_incremental() %}
where e._ingested_at >= (
    select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ntz) from {{ this }}
)
{% endif %}
