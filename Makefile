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
	go test $(PKG) -run '$(RUN)' -race -count=$(N) -failfast -timeout 0 2>&1 | tail -n 50; exit $${PIPESTATUS[0]}

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
