SHELL := /bin/sh
COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; elif docker-compose version >/dev/null 2>&1; then echo "docker-compose"; else echo "docker compose"; fi)
COMPOSE_RF3 ?= $(COMPOSE) -f compose.yaml -f compose.rf3.yaml
TABLE_COUNT ?= 5
SOURCE_BACKLOG_PER_TABLE ?= 1000
SINK_BACKLOG_PER_TABLE ?= 2000
OVERLOAD_BATCH_PER_TABLE ?= 1000
OVERLOAD_MAX_BATCHES ?= 100
MIN_SOURCE_LAG_BYTES ?= 65536
MIN_SOURCE_LAG_RECORDS_PER_PARTITION ?= 100
MIN_SINK_LAG_RECORDS_PER_PARTITION ?= 100
STABLE_LAG_SAMPLES ?= 3
MAX_ADMITTED_ROWS_PER_PARTITION ?= 50000
ADMISSION_TIMEOUT_SECONDS ?= 30
PRODLIKE_ACTIVE_BATCH_PER_TABLE ?= 100
PRODLIKE_RETIRING_BATCH_PER_TABLE ?= 1
PRODLIKE_ACTIVE_PAUSE_MS ?= 5
PRODLIKE_RETIRING_PAUSE_MS ?= 50
PRODLIKE_MAX_SOURCE_LAG_BYTES ?= 8388608
PRODLIKE_MAX_SINK_LAG_RECORDS ?= 10
PRODLIKE_PARK_BUDGET_MS ?= 200
PRODLIKE_REVERT_RESERVE_MS ?= 50

.PHONY: preflight test safety-coverage config config-rf3 up up-rf3 setup setup-rf3 benchmark benchmark-running benchmark-prodlike benchmark-prodlike-rf3 playground-api playground-api-rf3 playground-supervisor playground-ui playground playground-rf3 down reset reset-rf3 logs

preflight:
	@command -v docker >/dev/null || { echo "Docker is required"; exit 1; }
	@$(COMPOSE) version >/dev/null 2>&1 || { echo "Docker Compose is required but is not installed"; exit 1; }
	@test -f .env || { echo "Copy .env.example to .env and replace POSTGRES_PASSWORD"; exit 1; }
	@mode=$$(stat -f '%Lp' .env 2>/dev/null || stat -c '%a' .env 2>/dev/null); \
		test "$$mode" = 600 || { echo ".env must be mode 600; run: chmod 600 .env"; exit 1; }
	@! grep -q '^POSTGRES_PASSWORD=replace-with-local-only-password$$' .env || { echo "Replace the placeholder POSTGRES_PASSWORD in .env"; exit 1; }

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -p 'test_*.py' -v

safety-coverage:
	PYTHONPATH=src .venv/bin/python -m coverage run --branch \
		--source=flipbench.core,flipbench.settings,flipbench.connector_configs,flipbench.results,flipbench.playground_results,flipbench.playground_supervisor \
		-m unittest discover -s tests -p 'test_*.py'
	.venv/bin/python -m coverage report --fail-under=80

config: preflight
	$(COMPOSE) config --quiet

config-rf3: preflight
	$(COMPOSE_RF3) config --quiet

up: preflight
	$(COMPOSE) up -d --build hot warm kafka source-connect sink-connect

up-rf3: preflight
	$(COMPOSE_RF3) up -d --build hot warm kafka kafka-2 kafka-3 source-connect sink-connect

setup:
	$(COMPOSE) --profile tools run --rm --build runner setup --tables $(TABLE_COUNT)

setup-rf3:
	$(COMPOSE_RF3) --profile tools run --rm --build runner setup --tables $(TABLE_COUNT)

benchmark:
	$(COMPOSE) --profile tools run --rm --build runner benchmark \
		--tables $(TABLE_COUNT) \
		--source-events-per-table $(SOURCE_BACKLOG_PER_TABLE) \
		--sink-events-per-table $(SINK_BACKLOG_PER_TABLE)

benchmark-running:
	$(COMPOSE) --profile tools run --rm --build runner benchmark-running \
		--tables $(TABLE_COUNT) \
		--batch-events-per-table $(OVERLOAD_BATCH_PER_TABLE) \
		--max-batches $(OVERLOAD_MAX_BATCHES) \
		--min-source-lag-bytes $(MIN_SOURCE_LAG_BYTES) \
		--min-source-lag-records-per-partition $(MIN_SOURCE_LAG_RECORDS_PER_PARTITION) \
		--min-sink-lag-records-per-partition $(MIN_SINK_LAG_RECORDS_PER_PARTITION) \
		--stable-samples $(STABLE_LAG_SAMPLES) \
		--max-admitted-rows-per-partition $(MAX_ADMITTED_ROWS_PER_PARTITION) \
		--admission-timeout-seconds $(ADMISSION_TIMEOUT_SECONDS)

benchmark-prodlike:
	$(COMPOSE) --profile tools run --rm --build runner benchmark-prodlike \
		--tables $(TABLE_COUNT) \
		--active-events-per-table $(PRODLIKE_ACTIVE_BATCH_PER_TABLE) \
		--retiring-events-per-table $(PRODLIKE_RETIRING_BATCH_PER_TABLE) \
		--active-pause-ms $(PRODLIKE_ACTIVE_PAUSE_MS) \
		--retiring-pause-ms $(PRODLIKE_RETIRING_PAUSE_MS) \
		--max-source-lag-bytes $(PRODLIKE_MAX_SOURCE_LAG_BYTES) \
		--max-sink-lag-records-per-partition $(PRODLIKE_MAX_SINK_LAG_RECORDS) \
		--park-budget-ms $(PRODLIKE_PARK_BUDGET_MS) \
		--revert-reserve-ms $(PRODLIKE_REVERT_RESERVE_MS)

benchmark-prodlike-rf3:
	$(COMPOSE_RF3) --profile tools run --rm --build runner benchmark-prodlike \
		--tables $(TABLE_COUNT) \
		--active-events-per-table $(PRODLIKE_ACTIVE_BATCH_PER_TABLE) \
		--retiring-events-per-table $(PRODLIKE_RETIRING_BATCH_PER_TABLE) \
		--active-pause-ms $(PRODLIKE_ACTIVE_PAUSE_MS) \
		--retiring-pause-ms $(PRODLIKE_RETIRING_PAUSE_MS) \
		--max-source-lag-bytes $(PRODLIKE_MAX_SOURCE_LAG_BYTES) \
		--max-sink-lag-records-per-partition $(PRODLIKE_MAX_SINK_LAG_RECORDS) \
		--park-budget-ms $(PRODLIKE_PARK_BUDGET_MS) \
		--revert-reserve-ms $(PRODLIKE_REVERT_RESERVE_MS)

playground-api: preflight
	$(COMPOSE) --profile playground up -d --build playground-api

playground-api-rf3: preflight
	$(COMPOSE_RF3) --profile playground up -d --build playground-api

playground-supervisor: preflight
	PYTHONPATH=src python3 -m flipbench.playground_supervisor

playground-ui:
	cd playground-ui && npm run dev

playground: playground-api
	cd playground-ui && npm run dev

playground-rf3: playground-api-rf3
	@PYTHONPATH=src python3 -m flipbench.playground_supervisor & supervisor_pid=$$!; \
		trap 'kill $$supervisor_pid 2>/dev/null || true' EXIT INT TERM; \
		cd playground-ui && npm run dev

logs:
	$(COMPOSE) logs --tail=200 hot warm kafka source-connect sink-connect

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down --volumes

reset-rf3:
	$(COMPOSE_RF3) down --volumes
