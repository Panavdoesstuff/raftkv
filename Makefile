SHELL := /bin/bash
export PATH := /opt/homebrew/bin:$(PATH)

NODES ?= 3
N ?= 100
PKG ?= ./...
RUN ?= .

.PHONY: test stress chaos chaos-docker cluster-up cluster-down bench proto

test:
	go test ./... -race

# Repeat one test N times with -race; stop on first failure.
# Usage: make stress PKG=./internal/raft RUN=Election N=500
stress:
	@go test $(PKG) -run '$(RUN)' -race -count=$(N) -failfast -timeout 0 > stress.log 2>&1; rc=$$?; \
	  if [ $$rc -ne 0 ]; then grep -E '^(--- FAIL|FAIL|panic)|seed|DATA RACE' stress.log | head -n 40; fi; tail -n 20 stress.log; \
	  echo "stress: exit $$rc (full log: stress.log)"; exit $$rc

chaos:
	go run ./cmd/chaos -target=memnet -runs=$(N)

chaos-docker:
	go run ./cmd/chaos -target=docker -runs=$(N)

cluster-up:
	docker compose -f deploy/docker-compose.$(NODES).yml up -d --build

cluster-down:
	docker compose -f deploy/docker-compose.$(NODES).yml down -v

bench:
	go run ./cmd/bench -out=bench/results

proto:
	protoc --go_out=. --go_opt=module=github.com/panavdoesstuff/raftkv \
	       --go-grpc_out=. --go-grpc_opt=module=github.com/panavdoesstuff/raftkv \
	       proto/raft.proto proto/kv.proto proto/admin.proto
