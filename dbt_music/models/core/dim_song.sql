{{ config(materialized='table') }}

select
    s.song_id,
    s.song_name,
    s.artist_id,
    s.genre,
    s.duration_ms
from {{ ref('stg_songs') }} s
