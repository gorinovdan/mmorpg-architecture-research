SHELL := /bin/sh

.PHONY: up migrate test experiment verify-results down logs clean

up:
	docker compose up -d --build postgres redis backend worker

migrate:
	@echo "Migrations are applied automatically by researchd at startup."
	@curl -fsS http://localhost:18080/health >/dev/null

test:
	go test ./...
	docker compose config >/dev/null
	python3 -m py_compile scripts/run_experiments.py scripts/verify_results.py

experiment:
	python3 scripts/run_experiments.py --base-url http://localhost:18080 --ws-url ws://localhost:18080/ws?last=0-0 --out-dir results/latest

verify-results:
	python3 scripts/verify_results.py results/latest/summary.json

logs:
	docker compose logs --tail=120 backend worker

down:
	docker compose down -v

clean:
	rm -rf results/latest/*
