// Command server runs one RaftKV node: Raft over gRPC on --raft-port, and the
// KV and admin services on --client-port.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/panavdoesstuff/raftkv/internal/admin"
	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/storage"
	"github.com/panavdoesstuff/raftkv/internal/transport"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
	"github.com/panavdoesstuff/raftkv/proto/kvpb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "server:", err)
		os.Exit(1)
	}
}

// parsePeers parses "n1=host:port,n2=host:port".
func parsePeers(s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, addr, ok := strings.Cut(p, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", p)
		}
		out[raft.NodeID(id)] = addr
	}
	return out, nil
}

func run() error {
	var (
		id         = flag.String("id", "", "node id (required)")
		peers      = flag.String("peers", "", "raft addresses of the cluster: id=host:port,... (include this node unless --join)")
		raftPort   = flag.Int("raft-port", 7000, "port for Raft peer RPCs")
		clientPort = flag.Int("client-port", 8000, "port for KV client and admin RPCs")
		host       = flag.String("host", "", "interface to listen on (default: all)")
		dataDir    = flag.String("data-dir", "", "directory for the WAL and snapshots (required)")
		readMode   = flag.String("read-mode", "log", "how Gets are served: log|readindex|lease")
		join       = flag.Bool("join", false, "start as a non-voter with an empty log, to be added with add-server")
		seed       = flag.Int64("seed", 0, "seed for the fault injector's randomness (0: derive from the clock and pid)")
		maxLog     = flag.Int("max-log-bytes", kv.DefaultMaxLogBytes, "snapshot when the Raft log passes this many bytes")
	)
	flag.Parse()
	if *id == "" || *dataDir == "" {
		return fmt.Errorf("--id and --data-dir are required")
	}
	addrs, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	mode, err := kv.ParseReadMode(*readMode)
	if err != nil {
		return err
	}
	self := raft.NodeID(*id)
	if *seed == 0 {
		*seed = time.Now().UnixNano() ^ int64(os.Getpid())
	}

	level := slog.LevelInfo
	if os.Getenv("RAFT_DEBUG") == "1" {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})).With("node", *id)

	// The bootstrap voter set. A joining node leaves itself out, so it stays
	// passive until a leader adds it; it still needs the others' addresses.
	var voters []raft.NodeID
	for p := range addrs {
		if *join && p == self {
			continue
		}
		voters = append(voters, p)
	}
	sort.Slice(voters, func(i, j int) bool { return voters[i] < voters[j] })
	if !*join && addrs[self] == "" {
		return fmt.Errorf("--peers must include this node (%s) unless --join is set", *id)
	}

	fs, err := storage.OpenFile(*dataDir)
	if err != nil {
		return fmt.Errorf("open data dir: %w", err)
	}
	defer fs.Close()

	// The node's clock is a SkewedClock over the wall clock so the admin
	// service can skew it (Phase 9). At rate 1, offset 0 it is the real clock.
	// The same clock drives Raft's election and heartbeat deadlines, the lease
	// check and the KV server's timers, so a skew reaches all of them.
	clk := clock.NewSkewed(clock.RealClock{}, 0, 1)
	tr := transport.NewGRPC(self, fmt.Sprintf("%s:%d", *host, *raftPort), addrs)
	// The fault injector: every outgoing Raft RPC goes through the rules the
	// chaos controller sets over the admin SetNetRules RPC (none by default).
	faults := transport.NewFaultNet(*seed, clk)
	faults.SetLocal(self)
	defer faults.Close()
	store := kv.NewStore()
	node := raft.New(raft.Config{
		ID: self, Peers: voters, Transport: faults.Wrap(self, tr), Clock: clk, Storage: fs, SM: store,
		Seed: time.Now().UnixNano() ^ int64(os.Getpid()),
	})
	srv := kv.NewServer(node, store, clk, kv.WithMaxLogBytes(*maxLog), kv.WithReadMode(mode))

	if err := tr.Serve(self, node); err != nil {
		return fmt.Errorf("raft listener: %w", err)
	}
	defer tr.Close()
	if err := node.Start(); err != nil {
		return fmt.Errorf("start raft: %w", err)
	}
	go func() { // the state machine is applied by Raft itself; just drain notifications
		for range node.ApplyCh() {
		}
	}()

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *clientPort))
	if err != nil {
		return fmt.Errorf("client listener: %w", err)
	}
	gs := grpc.NewServer()
	kvpb.RegisterKVServer(gs, kv.GRPCService{S: srv})
	adminpb.RegisterAdminServer(gs, admin.New(node, clk, faults, log))
	serveErr := make(chan error, 1)
	go func() { serveErr <- gs.Serve(lis) }()

	log.Info("server up", "raft", tr.Addr(), "client", lis.Addr().String(), "read_mode", mode.String(),
		"join", *join, "data_dir", *dataDir, "voters", voters)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
	case err := <-serveErr:
		log.Error("client server failed", "err", err)
	}

	// Stop taking client requests (bounded: a request may be waiting on a
	// quorum that no longer exists), then stop Raft, then the listeners and
	// storage via the defers.
	stopped := make(chan struct{})
	go func() { gs.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		gs.Stop()
	}
	node.Stop()
	log.Info("stopped")
	return nil
}
