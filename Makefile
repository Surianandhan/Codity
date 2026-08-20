# Codity — distributed job scheduler.
# Every target is a thin wrapper over a command in README.md; nothing here is the
# only way to do anything. Run `make` for the list.
#
# macOS ships GNU Make 3.81, so this file stays inside 3.81 features.

BACKEND := backend
UV      := uv run

# The worker talks to Postgres directly, so it must be told which tenant it serves.
# Defaults to the first organization in the local database; override with
#   make worker ORG=<uuid>
ORG ?= $(shell psql -d codity -Atc "select id from organizations order by created_at limit 1" 2>/dev/null)

JOBS         ?= 500
FAILURE_RATE ?= 0.2
CONCURRENCY  ?= 4
PORT         ?= 8000

.DEFAULT_GOAL := help
.PHONY: help setup migrate seed api worker worker2 scheduler test test-concurrency demo check lint typecheck cov

help: ## Show this list
	@grep -E '^[a-z][a-zA-Z0-9_-]*:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Nothing executes without a running worker: make api, make worker, make scheduler."

# --- setup -------------------------------------------------------------------

setup: ## Create both databases and sync Python dependencies
	-createdb codity
	-createdb codity_test
	cd $(BACKEND) && uv sync

migrate: ## Apply all Alembic migrations to the dev database
	cd $(BACKEND) && $(UV) alembic upgrade head

seed: ## Insert a demo org, project, queues and handlers
	cd $(BACKEND) && $(UV) python scripts/seed.py

# --- processes ---------------------------------------------------------------

api: ## Run the API on :8000 with reload
	cd $(BACKEND) && $(UV) uvicorn app.main:app --reload --port $(PORT)

worker: ## Run worker-1 (override tenant with ORG=<uuid>)
	@test -n "$(ORG)" || { echo "ORG is empty: pass ORG=<uuid> or run 'make seed' first"; exit 1; }
	cd $(BACKEND) && $(UV) python -m app.worker.main --org $(ORG) --name worker-1 --concurrency $(CONCURRENCY)

worker2: ## Run worker-2 -- the second worker the kill -9 demo needs
	@test -n "$(ORG)" || { echo "ORG is empty: pass ORG=<uuid> or run 'make seed' first"; exit 1; }
	cd $(BACKEND) && $(UV) python -m app.worker.main --org $(ORG) --name worker-2 --concurrency $(CONCURRENCY)

scheduler: ## Run the scheduler: promoter, cron, reaper, sweeps
	cd $(BACKEND) && $(UV) python -m app.scheduler.main

# --- verification ------------------------------------------------------------

test: ## Run the full test suite against codity_test
	cd $(BACKEND) && $(UV) pytest -q

test-concurrency: ## Run only the concurrency tests (real committed sessions)
	cd $(BACKEND) && $(UV) pytest -q -m concurrency

demo: ## Enqueue load and print the end-of-run invariant block
	cd $(BACKEND) && $(UV) python scripts/demo_load.py --jobs $(JOBS) --failure-rate $(FAILURE_RATE)

lint: ## ruff
	cd $(BACKEND) && $(UV) ruff check app/ tests/

typecheck: ## mypy, strict
	cd $(BACKEND) && $(UV) mypy app/

cov: ## Test suite with a coverage report (no gate -- see docs/TESTING.md)
	cd $(BACKEND) && uv run --with pytest-cov pytest -q --cov=app --cov-report=term-missing

check: lint typecheck test ## lint + typecheck + tests. Run before every commit.
