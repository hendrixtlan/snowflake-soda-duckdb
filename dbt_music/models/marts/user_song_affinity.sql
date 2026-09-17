{#-
  Implicit-feedback matrix for collaborative filtering.

  The raw score weighs the signals a listener gives without being asked; it is
  unbounded and can go negative, so it is min-max normalised WITHIN each user.
  Normalising per user rather than globally keeps a light listener's preferences
  comparable to a heavy one's: the score means "how much does THIS user like
  this song relative to their own catalogue", which is what a recommender needs.

  The weights are a deliberate, auditable choice, not a learned parameter. An ML
  team can replace this block without touching the rest of the pipeline.
-#}
{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('fct_listening_events') }}
    where event_ts >= dateadd('day', -30, current_timestamp())
      and not is_orphan_song
),

raw_scores as (
    select
        user_id,
        song_id,
        count_if(event_type = 'play')   as play_count,
        count_if(event_type = 'skip')   as skip_count,
        count_if(event_type = 'replay') as replay_count,
        count_if(event_type = 'like')   as like_count,
        avg(completion_rate)            as avg_completion_rate,
        (
              1.00 * count_if(event_type = 'play')
            + 1.50 * count_if(event_type = 'replay')
            + 2.00 * count_if(event_type = 'like')
            - 1.25 * count_if(event_type = 'skip')
            + 2.00 * avg(completion_rate)
        ) as raw_score
    from base
    group by user_id, song_id
),

bounds as (
    select
        user_id,
        min(raw_score) as min_score,
        max(raw_score) as max_score
    from raw_scores
    group by user_id
)

select
    r.user_id,
    r.song_id,
    r.play_count,
    r.skip_count,
    r.replay_count,
    r.like_count,
    r.avg_completion_rate,
    r.raw_score,
    case
        when b.max_score = b.min_score then 1.0
        else (r.raw_score - b.min_score) / (b.max_score - b.min_score)
    end as affinity_score
from raw_scores r
join bounds b using (user_id)
