SHELL := /bin/bash
DB_URL = $(shell scripts/devdb.sh url)

.PHONY: help db-up db-down build test test-race vet dataset bench bench-one bench-report bench-compare bench-charts sim api clean

help:
	@echo "db-up        start the project-local PostgreSQL 16 cluster (.devdb/)"
	@echo "build        build bin/api, bin/bench, bin/stripesim, bin/dataset"
	@echo "test         unit + integration tests (needs db-up)"
	@echo "dataset      download, verify and normalize the ULB credit-card dataset"
	@echo "bench        run every simulator scenario 3x into bench/results"
	@echo "bench-one    SCENARIO=bench/scenarios/04-ambiguous-timeouts.json REPEAT=1"
	@echo "bench-report print the markdown summary of bench/results"
	@echo "bench-compare / bench-charts  recorded suite comparison / regenerate docs/img and docs/results"
	@echo "sim / api    run the Stripe simulator / the API against it (two terminals)"

db-up:
	scripts/devdb.sh start

db-down:
	scripts/devdb.sh stop

build:
	go build -o bin/api ./cmd/api
	go build -o bin/bench ./cmd/bench
	go build -o bin/stripesim ./cmd/stripesim
	go build -o bin/dataset ./cmd/dataset

vet:
	go vet ./...

test:
	TEST_DATABASE_URL="$(DB_URL)" go test ./...

test-race:
	TEST_DATABASE_URL="$(DB_URL)" go test -race -count=1 ./...

dataset:
	go run ./cmd/dataset fetch-ulb -dir data

bench: build
	./bin/bench run -scenario 'bench/scenarios/0*.json' -repeat 3 -admin-db "$(DB_URL)" -out bench/results

SCENARIO ?= bench/scenarios/01-baseline.json
REPEAT ?= 1
bench-one: build
	./bin/bench run -scenario '$(SCENARIO)' -repeat $(REPEAT) -admin-db "$(DB_URL)" -out bench/results

bench-report:
	go run ./cmd/bench report -dir bench/results

sim:
	go run ./cmd/stripesim -addr 127.0.0.1:12111 \
		-webhook-url http://127.0.0.1:8080/v1/webhooks/stripe -webhook-secret whsec_dev

api:
	DATABASE_URL="$(DB_URL)" API_KEYS=dev_key \
	STRIPE_API_BASE=http://127.0.0.1:12111 STRIPE_SECRET_KEY=sk_test_dev STRIPE_WEBHOOK_SECRET=whsec_dev \
	go run ./cmd/api

clean:
	rm -rf bin

# Compare the single-row merchant payable baseline with the current code
# (the interleaved suite recorded in the repository; see docs/BENCHMARKS.md).
bench-compare:
	go run ./cmd/bench compare -before bench/sweeps/suite-3/baseline -after bench/sweeps/suite-3/adaptive

# Tables and charts from the recorded sweeps.
bench-charts:
	scripts/make-charts.sh
