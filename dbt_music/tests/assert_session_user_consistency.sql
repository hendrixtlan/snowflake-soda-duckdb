select session_id
from {{ ref('stg_listening_events') }}
group by session_id
having count(distinct user_id) > 1
