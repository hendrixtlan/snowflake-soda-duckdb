{{ config(materialized='table') }}

select
    a.artist_id,
    a.artist_name,
    a.genre
from {{ ref('stg_artists') }} a
