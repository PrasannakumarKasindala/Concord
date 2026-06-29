.DEFAULT_GOAL := help
.PHONY: help install test lint bench plot demo up down fmt

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install: ## install package with dev extras (editable)
	pip install -e ".[dev]"

test: ## run tests with coverage gate
	pytest --cov=concord --cov-report=term-missing --cov-fail-under=85

lint: ## static checks
	ruff check concord tests benchmark

demo: ## run the in-memory demo (no infra needed)
	concord demo --count 500 --fail-rate 0.15

bench: ## run the load simulation and write results
	python -m benchmark.harness

plot: ## render the latency chart from the last bench run
	python -m benchmark.plot

up: ## start local infra + relay (Postgres, Redpanda, Redis, MinIO)
	docker compose up --build

down: ## tear down local infra and volumes
	docker compose down -v
