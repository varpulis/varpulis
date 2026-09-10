#!/usr/bin/env bash
# Three-node NATS JetStream cluster on localhost, for testing the control plane
# with the replication a deployment that has replaced Raft actually has.
#
# A single `nats-server -js` exercises the control-plane logic but proves
# nothing about durability: the bucket lives on one process, and killing it
# takes the whole control plane with it. Only a replicated bucket can be asked
# the question Raft was there to answer.
#
#   scripts/nats-cluster.sh start          # ports 4231/4232/4233
#   scripts/nats-cluster.sh kill 1         # stop one node, quorum survives
#   scripts/nats-cluster.sh start-one 1    # bring it back
#   scripts/nats-cluster.sh status
#   scripts/nats-cluster.sh stop
#
# Then:
#   VARPULIS_TEST_NATS_URL=nats://127.0.0.1:4231 \
#     VARPULIS_CONTROL_PLANE_REPLICAS=3 VARPULIS_REQUIRE_BROKERS=1 \
#     cargo test -p varpulis-cluster --features jetstream-control-plane \
#     --test jetstream_control_plane
set -euo pipefail

DIR="${VARPULIS_NATS_CLUSTER_DIR:-${TMPDIR:-/tmp}/varpulis-nats-cluster}"
BIN="${NATS_SERVER:-nats-server}"

client_port() { echo $((4230 + $1)); }
cluster_port() { echo $((6230 + $1)); }

write_conf() {
    local i=$1
    mkdir -p "$DIR/js$i"
    cat > "$DIR/n$i.conf" <<EOF
port: $(client_port "$i")
server_name: n$i
jetstream { store_dir: "$DIR/js$i", max_mem: 128MB, max_file: 1GB }
cluster {
  name: varpulis-test
  listen: 127.0.0.1:$(cluster_port "$i")
  routes: [ nats://127.0.0.1:$(cluster_port 1), nats://127.0.0.1:$(cluster_port 2), nats://127.0.0.1:$(cluster_port 3) ]
}
EOF
}

start_one() {
    local i=$1
    write_conf "$i"
    "$BIN" -c "$DIR/n$i.conf" > "$DIR/n$i.log" 2>&1 &
    echo "$!" > "$DIR/n$i.pid"
    echo "started n$i (pid $(cat "$DIR/n$i.pid"), client $(client_port "$i"))"
}

case "${1:-}" in
start)
    command -v "$BIN" >/dev/null || {
        echo "error: $BIN not on PATH. Install nats-server, or set NATS_SERVER." >&2
        exit 1
    }
    mkdir -p "$DIR"
    for i in 1 2 3; do start_one "$i"; done
    # Wait for the JetStream meta group to elect a leader; without it the very
    # first bucket create races and times out.
    for _ in $(seq 60); do
        if grep -qs "JetStream cluster new metadata leader\|Self is new JetStream cluster metadata leader" "$DIR"/n*.log; then
            echo "meta leader elected"
            exit 0
        fi
        sleep 0.5
    done
    echo "error: no JetStream metadata leader after 30s; see $DIR/n*.log" >&2
    exit 1
    ;;
start-one) start_one "${2:?which node}" ;;
kill)
    i="${2:?which node}"
    kill "$(cat "$DIR/n$i.pid")" 2>/dev/null && echo "killed n$i" || echo "n$i not running"
    rm -f "$DIR/n$i.pid"
    ;;
stop)
    for i in 1 2 3; do
        [ -f "$DIR/n$i.pid" ] && kill "$(cat "$DIR/n$i.pid")" 2>/dev/null || true
        rm -f "$DIR/n$i.pid"
    done
    echo "stopped"
    ;;
status)
    for i in 1 2 3; do
        if [ -f "$DIR/n$i.pid" ] && kill -0 "$(cat "$DIR/n$i.pid")" 2>/dev/null; then
            echo "n$i up   (client $(client_port "$i"))"
        else
            echo "n$i down"
        fi
    done
    ;;
*)
    sed -n '2,25p' "$0"
    exit 1
    ;;
esac
