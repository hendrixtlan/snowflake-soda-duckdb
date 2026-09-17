{#-
  The guard against the failure mode this project exists to expose: a clean
  build that is clean only because the pipeline threw the evidence away.
  Fails if more than 0.1% of staged events never reached the fact table.
-#}
select
    staged.n as staged_events,
    fact.n   as fact_events
from
    (select count(*) as n from {{ ref('stg_listening_events') }}) staged,
    (select count(*) as n from {{ ref('fct_listening_events') }}) fact
where (staged.n - fact.n) > staged.n * 0.001
