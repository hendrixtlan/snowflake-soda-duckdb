# Build plan

## Milestone 1 — analytical reliability path
Synthetic generator → GE → Snowflake RAW → dbt STAGING/CORE/MARTS → Soda.

## Milestone 2 — behavioral observability
Connect Monte Carlo to Snowflake and establish a 14-day clean baseline.
Run A8-A12 separately and record time-to-detection and lineage.

## Milestone 3 — streaming path
Kafka → Flink → S3. Lambda triggers the GE gate before Snowflake loading.

## Milestone 4 — serving
Java 17 / Spring Boot API for certified user features and dataset status.

## Milestone 5 — operations
Prometheus/Grafana/OpenSearch and incident persistence.
