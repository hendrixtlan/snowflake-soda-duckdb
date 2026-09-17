{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('fct_listening_events') }}
    where event_ts >= dateadd('day', -30, current_timestamp())
      and not is_orphan_song
)

select
    user_id,
    genre,
    count(*)                                                          as events,
    sum(ms_played) / 60000.0                                          as minutes_listened,
    count_if(event_type = 'skip') / nullif(count(*), 0)::float        as skip_rate,
    sum(ms_played) / nullif(sum(sum(ms_played)) over (partition by user_id), 0) as affinity_score
from base
group by user_id, genre
