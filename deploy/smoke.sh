#!/usr/bin/env bash
# Docker exit checks for Phase 8. Needs a running Docker daemon.
#   deploy/smoke.sh            3-node cluster
#   NODES=5 deploy/smoke.sh    5-node cluster
#   KEEP=1 deploy/smoke.sh     leave the cluster up afterwards
# Steps: cluster-up; docker kill the leader and check writes resume; docker
# restart a node and check it catches up from disk; add a 4th container with
# add-server, then remove it. Exits non-zero at the first failed check.
set -euo pipefail
cd "$(dirname "$0")/.."

NODES=${NODES:-3}
JOINER=$((NODES + 1))
F=deploy/docker-compose.$NODES.yml
NET=raftkv${NODES}_default
dc() { docker compose -f "$F" "$@"; }

SERVERS=""
for i in $(seq "$NODES"); do SERVERS+="${SERVERS:+,}n$i=n$i:8000"; done
# cli runs the client binary from the image, inside the cluster network, so it
# works no matter which nodes are down.
cli() { docker run --rm --network "$NET" --entrypoint /usr/local/bin/client raftkv:latest --servers "$SERVERS" "$@"; }
fail() { echo "SMOKE FAIL: $*" >&2; dc logs --tail=30 >&2 || true; exit 1; }
step() { echo "== $*"; }

cleanup() { [ "${KEEP:-0}" = 1 ] || make cluster-down NODES="$NODES" >/dev/null 2>&1 || true; }
trap cleanup EXIT

leader() { cli status 2>/dev/null | awk '/role=leader/ {print $1; exit}'; }
wait_leader() {
  local l=""
  for _ in $(seq 60); do l=$(leader || true); [ -n "$l" ] && { echo "$l"; return; }; sleep 1; done
  fail "no leader elected"
}
# applied <node>: the node's last-applied index from status.
applied() { cli status 2>/dev/null | awk -v n="$1" '$1 == n { for (i = 1; i <= NF; i++) if ($i ~ /^applied=/) { sub("applied=", "", $i); print $i } }'; }
# commit_of <node>
commit_of() { cli status 2>/dev/null | awk -v n="$1" '$1 == n { for (i = 1; i <= NF; i++) if ($i ~ /^commit=/) { sub("commit=", "", $i); print $i } }'; }

step "cluster-up ($NODES nodes)"
make cluster-up NODES="$NODES" >/dev/null
L=$(wait_leader); echo "leader: $L"
cli put smoke 1 >/dev/null || fail "initial put"
[ "$(cli get smoke)" = 1 ] || fail "initial get"

step "docker kill the leader ($L): writes must resume"
dc kill "$L"
cli put after-kill ok >/dev/null || fail "write did not resume after killing $L"
[ "$(cli get after-kill)" = ok ] || fail "read after failover"
[ "$(cli get smoke)" = 1 ] || fail "data lost across failover"
dc start "$L"
sleep 3

step "docker restart a node: it catches up from disk"
V=$(for i in $(seq "$NODES"); do if [ "n$i" != "$L" ]; then echo "n$i"; fi; done | head -1)
cli put before-restart 1 >/dev/null
dc restart "$V"
cli put while-restarting 1 >/dev/null || fail "write after restarting $V"
ok=0
for _ in $(seq 40); do
  a=$(applied "$V" || true); c=$(commit_of "$(wait_leader)" || true)
  if [ -n "$a" ] && [ -n "$c" ] && [ "$a" -ge $((c - 1)) ]; then ok=1; break; fi
  sleep 1
done
[ "$ok" = 1 ] || fail "$V did not catch up (applied=$a leader commit=$c)"

step "add a ${JOINER}th container with add-server, then remove it"
dc --profile join up -d "n$JOINER"
cli add-server "n$JOINER" "n$JOINER:7000" "n$JOINER:8000" >/dev/null || fail "add-server"
cli status | grep -q "n$JOINER voter" || fail "n$JOINER is not a voter after add-server"
cli put with-joiner 1 >/dev/null || fail "write with the extra node"
cli remove-server "n$JOINER" >/dev/null || fail "remove-server"
cli status | grep -q "n$JOINER voter" && fail "n$JOINER still a voter after remove-server"
dc stop "n$JOINER" >/dev/null
[ "$(cli get with-joiner)" = 1 ] || fail "data lost after removal"

echo "SMOKE OK"
