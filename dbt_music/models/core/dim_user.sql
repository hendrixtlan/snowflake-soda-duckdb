{{ config(materialized='table') }}

select
    user_id,
    min(event_ts)                as first_event_ts,
    max(event_ts)                as last_event_ts,
    count(*)                     as lifetime_events,
    max(country)                 as country,
    count(distinct session_id)   as lifetime_sessions
from {{ ref('stg_listening_events') }}
where user_id is not null
group by user_id
