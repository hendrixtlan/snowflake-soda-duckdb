# Monte Carlo integration

This directory is intentionally configuration-first.

The MVP produces Snowflake tables and dbt artifacts first. Once the Snowflake
warehouse is connected to Monte Carlo, create monitors for:

1. RAW.LISTENING_EVENTS row-count / freshness
2. event_type distribution
3. country distribution
4. device_type distribution
5. CORE.FCT_LISTENING_EVENTS freshness
6. MARTS.USER_LISTENING_FEATURES row-count and freshness
7. MARTS.USER_SONG_AFFINITY row-count

The behavioral anomalies A8-A12 are the primary Monte Carlo experiments.
Do not duplicate deterministic GE/dbt/Soda rules here.
