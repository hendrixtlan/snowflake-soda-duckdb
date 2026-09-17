{#-
  Feature set consumed by the recommender. Windowed to 30 days: a lifetime
  aggregate would bury a recent change of taste under years of history.

  `feature_eligible` is a property of the user (enough signal to profile them).
  Dataset certification is a property of the DATASET and is decided by Soda,
  not here -- see MARTS.data_quality_status.
-#}
{{ config(materialized='table') }}

{% set window_days = 30 %}

with base as (
    select *
    from {{ ref('fct_listening_events') }}
    where event_ts >= dateadd('day', -{{ window_days }}, current_timestamp())
      and not is_orphan_song
),

per_user as (
    select
        user_id,
        count(*)                                                as total_events_30d,
        sum(ms_played) / 60000.0                                as total_minutes_30d,
        count(distinct song_id)                                 as distinct_songs_30d,
        count(distinct artist_id)                               as distinct_artists_30d,
        count(distinct session_id)                              as sessions_30d,
        count_if(event_type = 'play')                           as play_count,
        count_if(event_type = 'skip')                           as skip_count,
        count_if(event_type = 'replay')                         as replay_count,
        count_if(event_type = 'like')                           as like_count,
        count_if(event_type in ('play','skip','pause','replay')) as listening_events,
        avg(case when event_type in ('play','skip','pause','replay')
                 then completion_rate end)                      as avg_completion_rate,
        max(event_ts)                                           as last_event_ts
    from base
    group by user_id
),

session_len as (
    select user_id, avg(session_minutes) as avg_session_minutes
    from (
        select user_id, session_id,
               datediff('second', min(event_ts), max(event_ts)) / 60.0 as session_minutes
        from base
        group by user_id, session_id
    )
    group by user_id
),

genre_share as (
    select
        user_id,
        genre,
        sum(ms_played) as genre_ms,
        sum(ms_played) / nullif(sum(sum(ms_played)) over (partition by user_id), 0) as share
    from base
    group by user_id, genre
),

genre_stats as (
    select
        user_id,
        -- Shannon entropy normalised by log(n_genres): 0 = one genre only,
        -- 1 = listening spread evenly across the catalogue.
        - sum(share * ln(nullif(share, 0))) / ln({{ var('n_genres', 12) }}) as genre_diversity
    from genre_share
    group by user_id
),

top_genre as (
    select user_id, genre as top_genre, share as top_genre_share
    from (
        select user_id, genre, share,
               row_number() over (partition by user_id order by share desc, genre) as rn
        from genre_share
    )
    where rn = 1
),

top_artist as (
    select user_id, artist_id as top_artist_id
    from (
        select user_id, artist_id,
               row_number() over (partition by user_id order by sum(ms_played) desc, artist_id) as rn
        from base
        group by user_id, artist_id
    )
    where rn = 1
)

select
    u.user_id,
    u.total_events_30d,
    u.total_minutes_30d,
    u.distinct_songs_30d,
    u.distinct_artists_30d,
    u.sessions_30d,
    coalesce(sl.avg_session_minutes, 0)                          as avg_session_minutes,
    u.avg_completion_rate,
    u.skip_count   / nullif(u.listening_events, 0)::float        as skip_rate,
    u.replay_count / nullif(u.play_count, 0)::float              as replay_rate,
    u.like_count   / nullif(u.distinct_songs_30d, 0)::float      as like_rate,
    tg.top_genre,
    tg.top_genre_share,
    coalesce(gs.genre_diversity, 0)                              as genre_diversity,
    ta.top_artist_id,
    datediff('day', u.last_event_ts, current_timestamp())        as days_since_last_event,
    case
        when u.total_events_30d >= 200 then 'heavy'
        when u.total_events_30d >= 50  then 'regular'
        when u.total_events_30d >= 5   then 'casual'
        else 'dormant'
    end                                                          as activity_tier,
    u.total_events_30d >= 5                                      as feature_eligible,
    current_timestamp()                                          as feature_computed_at
from per_user u
left join session_len sl using (user_id)
left join genre_stats gs using (user_id)
left join top_genre   tg using (user_id)
left join top_artist  ta using (user_id)
