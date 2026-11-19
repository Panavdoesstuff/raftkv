SHELL := /bin/bash
export PATH := /opt/homebrew/bin:$(PATH)

NODES ?= 3
N ?= 100
PKG ?= ./...
RUN ?= .
DUR ?= 20s
SEED ?= 0
SCENARIO ?= random

.PHONY: test integration stress chaos chaos-docker cluster-up cluster-down bench proto

test:
	go test ./... -race

# Real multi-process cluster test (builds cmd/server and cmd/client). Slow, so
# it is behind the integration build tag and not part of `make test`.
integration:
	go test -tags integration -count=1 -timeout 5m ./test/integration

# Repeat one test N times with -race; stop on first failure.
# Usage: make stress PKG=./internal/raft RUN=Election N=500
stress:
	@go test $(PKG) -run '$(RUN)' -race -count=$(N) -failfast -timeout 0 > stress.log 2>&1; rc=$$?; \
	  if [ $$rc -ne 0 ]; then grep -E '^(--- FAIL|FAIL|panic)|seed|DATA RACE' stress.log | head -n 40; fi; tail -n 20 stress.log; \
	  echo "stress: exit $$rc (full log: stress.log)"; exit $$rc

# N random scenario/seed runs on memnet (SCENARIO=name to fix the scenario,
# SEED=n to reproduce; the seed is printed). Prints a per-scenario summary and
# exits non-zero if any run failed. Run directories go to chaos-runs/.
chaos:
	@go run ./cmd/chaos -target=memnet -runs=$(N) -duration=$(DUR) -seed=$(SEED) -scenario=$(SCENARIO); rc=$$?; echo "chaos: exit $$rc"; exit $$rc

# The same on the Docker cluster (rebuilt for every run; needs a running daemon).
chaos-docker:
	@go run ./cmd/chaos -target=docker -runs=$(N) -duration=$(DUR) -seed=$(SEED) -scenario=$(SCENARIO); rc=$$?; echo "chaos: exit $$rc"; exit $$rc

# NODES=3 (default) or NODES=5. The node-N joiner (profile "join") is started by
# deploy/smoke.sh, not here.
cluster-up:
	docker compose -f deploy/docker-compose.$(NODES).yml up -d --build

cluster-down:
	docker compose -f deploy/docker-compose.$(NODES).yml --profile join down -v --remove-orphans

bench:
	go run ./cmd/bench -out=bench/results

proto:
	protoc --go_out=. --go_opt=module=github.com/panavdoesstuff/raftkv \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/panavdoesstuff/raftkv \
	       proto/raft.proto proto/kv.proto proto/admin.proto
