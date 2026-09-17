-- Affinity shares must partition each user's listening time exactly.
select user_id, sum(affinity_score) as total
from {{ ref('user_genre_profile') }}
group by user_id
having abs(sum(affinity_score) - 1) > 0.001
