PYTHON  ?= python3
ANOMALY ?= A9
DAYS    ?= 1
AS_OF   ?=
DATASET ?= data/generated/listening_events.parquet

SODA_SCAN := experiment/results/soda_scan.json
GEN_FLAGS := $(if $(AS_OF),--as-of $(AS_OF),)

.PHONY: help setup catalog generate baseline validate load transform quality certify \
        pipeline inject experiment experiment-all matrix test test-models verify lint clean

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sed 's/:.*## /\t/' | expand -t24

setup:  ## Install Python dependencies and dbt packages
	$(PYTHON) -m pip install -r requirements.txt
	cd dbt_music && dbt deps --profiles-dir .

catalog:  ## Regenerate the song and artist catalogue
	$(PYTHON) -m generator.catalog

generate:  ## Generate a batch of synthetic events (DAYS=n)
	$(PYTHON) -m generator.events --days $(DAYS) $(GEN_FLAGS) --output $(DATASET)

baseline:  ## Generate 14 days of clean history for Monte Carlo to learn from
	$(PYTHON) -m generator.events --days 14 $(GEN_FLAGS) \
		--output data/generated/baseline.parquet

validate:  ## Run the ingestion contract over DATASET
	$(PYTHON) ingestion/great_expectations/validate_batch.py --input $(DATASET)

snowflake-init:  ## Create databases, schemas, roles and tables
	$(PYTHON) ingestion/run_sql.py infra/snowflake/init.sql

load:  ## Load the validated batch into RAW (idempotent)
	$(PYTHON) ingestion/load_to_snowflake.py \
		--events $(basename $(DATASET)).clean.parquet \
		--quarantine $(basename $(DATASET)).quarantine.parquet \
		--validation-report $(basename $(DATASET)).validation.json \
		--songs data/catalog/songs.csv --artists data/catalog/artists.csv

transform:  ## Build and test every dbt model
	cd dbt_music && dbt build --profiles-dir .
	cd dbt_music && dbt source freshness --profiles-dir .

quality:  ## Run every Soda check
	@mkdir -p experiment/results
	soda scan -d snowflake -c soda/configuration.yml -srf $(SODA_SCAN) \
		soda/checks/raw.yml soda/checks/core.yml soda/checks/marts.yml

certify:  ## Write dataset certification from the Soda scan result
	$(PYTHON) ingestion/certify.py --scan-result $(SODA_SCAN)

pipeline: generate validate load transform quality certify  ## Full clean run

inject:  ## Write an injected batch without running the pipeline
	$(PYTHON) -m generator.anomalies --input data/generated/baseline.parquet \
		--anomaly $(ANOMALY) --output data/generated/experiment_$(ANOMALY).parquet

experiment:  ## Inject ANOMALY and measure which controls detect it
	$(PYTHON) experiment/run_injection.py --anomaly $(ANOMALY)

experiment-all:  ## Run all 14 anomalies end to end
	@for a in $$(seq 1 14); do \
		echo "=== A$$a ==="; \
		$(PYTHON) experiment/run_injection.py --anomaly A$$a || exit 1; \
	done

matrix:  ## Build the detection matrix from measured runs
	$(PYTHON) experiment/build_matrix.py

test:  ## Run the test suite
	$(PYTHON) -m pytest tests/ -q

test-models:  ## Run the real dbt SQL against generated data in DuckDB
	$(PYTHON) -m pytest tests/test_model_semantics.py -q

lint:  ## Parse the dbt project and check the SQL against the Snowflake grammar
	cd dbt_music && dbt parse --profiles-dir . --no-partial-parse
	cd dbt_music && sqlfluff lint models/ tests/
	$(PYTHON) -m compileall -q generator ingestion experiment

verify: lint test  ## Everything that can be checked without a warehouse

clean:  ## Remove generated data, keeping the directory
	find data/generated -type f ! -name .gitkeep -delete
	rm -rf dbt_music/target dbt_music/logs
