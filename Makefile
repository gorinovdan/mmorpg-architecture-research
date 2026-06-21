SHELL := /bin/sh
PYTHON := .venv/bin/python

.PHONY: deps up migrate test experiment verify-results down logs clean

deps:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d --build postgres redis backend worker

migrate:
	@echo "Migrations are applied automatically by researchd at startup."
	@curl -fsS http://localhost:18080/health >/dev/null

test:
	go test ./...
	docker compose config >/dev/null
	$(PYTHON) -m py_compile scripts/run_experiments.py scripts/verify_results.py

experiment:
	$(PYTHON) scripts/run_experiments.py --base-url http://localhost:18080 --ws-url ws://localhost:18080/ws?last=0-0 --out-dir results/latest

verify-results:
	$(PYTHON) scripts/verify_results.py results/latest/summary.json

logs:
	docker compose logs --tail=120 backend worker

down:
	docker compose down -v

clean:
	rm -rf results/latest/*
