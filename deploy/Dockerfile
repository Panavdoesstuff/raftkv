# Multi-stage build: compile static binaries, ship only them on a tiny base.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/client ./cmd/client

FROM alpine:3.20
COPY --from=build /out/server /out/client /usr/local/bin/
VOLUME /data
EXPOSE 7000 8000
# Raft peer RPCs on 7000, KV client + admin RPCs on 8000.
ENTRYPOINT ["/usr/local/bin/server", "--data-dir=/data", "--raft-port=7000", "--client-port=8000"]
