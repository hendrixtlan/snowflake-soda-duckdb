-- Coverage: a user with activity in the window but no profile is invisible to
-- the recommender. Structurally wrong, not a threshold question.
select f.user_id
from (
    select distinct user_id
    from {{ ref('fct_listening_events') }}
    where event_ts >= dateadd('day', -30, current_timestamp())
      and not is_orphan_song
) f
left join {{ ref('user_listening_features') }} u on f.user_id = u.user_id
where u.user_id is null
