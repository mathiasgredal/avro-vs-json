SHELL := /bin/bash

BROKERS ?= localhost:9094
MESSAGES ?= 10000
BATCH ?= 100
COMPRESSION ?= none

.PHONY: up down topics node-install py-venv py-install go-build node-json node-avro py-json py-avro go-json go-avro clean

up:
	docker compose up -d
	@echo "Waiting for Kafka to be healthy..."
	bash scripts/wait_for_kafka.sh
	bash scripts/create_topics.sh

down:
	docker compose down -v

topics:
	bash scripts/create_topics.sh

node-install:
	cd node && npm install

py-venv:
	python3 -m venv .venv

py-install: py-venv
	. .venv/bin/activate && pip install -U pip && pip install -r requirements.txt

go-build:
	cd go && go mod download && go build -o bench bench.go

node-json:
	cd node && node bench.js --mode json --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

node-avro:
	cd node && node bench.js --mode avro --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

py-json:
	. .venv/bin/activate && python python/bench.py --mode json --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

py-avro:
	. .venv/bin/activate && python python/bench.py --mode avro --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

go-json: go-build
	./go/bench --mode json --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

go-avro: go-build
	./go/bench --mode avro --messages $(MESSAGES) --batch $(BATCH) --compression $(COMPRESSION) --brokers $(BROKERS)

clean:
	rm -rf .venv node/node_modules go/bench results bench_results || true


