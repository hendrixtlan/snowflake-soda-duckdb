{#-
  Every row the transformation layer discards, with the reason, in one place.
  This model is the antidote to silent absorption: dbt tests and Soda checks
  run against it, so anything dropped on the way to CORE has to be accounted for.
-#}
{{ config(materialized='view') }}

with events as (
    select * from {{ ref('stg_listening_events') }}
),

duplicates as (
    select
        event_id,
        _batch_id,
        'duplicate_event_id' as rejection_reason
    from events
    where was_deduplicated
),

orphan_songs as (
    select
        e.event_id,
        e._batch_id,
        'song_id_not_in_catalog' as rejection_reason
    from events e
    left join {{ ref('stg_songs') }} s on e.song_id = s.song_id
    where s.song_id is null
),

orphan_artists as (
    select
        e.event_id,
        e._batch_id,
        'artist_id_not_in_catalog' as rejection_reason
    from events e
    left join {{ ref('stg_artists') }} a on e.artist_id = a.artist_id
    where a.artist_id is null
),

catalog_mismatch as (
    select
        e.event_id,
        e._batch_id,
        'song_catalog_attribute_mismatch' as rejection_reason
    from events e
    join {{ ref('stg_songs') }} s on e.song_id = s.song_id
    where e.artist_id <> s.artist_id or e.genre <> s.genre
)

select * from duplicates
union all select * from orphan_songs
union all select * from orphan_artists
union all select * from catalog_mismatch
