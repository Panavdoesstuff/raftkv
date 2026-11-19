// Command client is the RaftKV CLI.
//
//	client --servers n1=h:p,n2=h:p get KEY
//	client --servers ... put KEY VALUE
//	client --servers ... append KEY VALUE
//	client --servers ... add-server ID RAFT_ADDR [CLIENT_ADDR]
//	client --servers ... remove-server ID
//	client --servers ... status
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

const usage = `usage: client [--servers id=host:port,...] [--timeout 30s] COMMAND
  get KEY | put KEY VALUE | append KEY VALUE
  add-server ID RAFT_ADDR [CLIENT_ADDR] | remove-server ID | status`

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// parseServers parses "n1=host:port,host:port". A bare address names itself.
func parseServers(s string) ([]raft.NodeID, map[raft.NodeID]string, error) {
	var ids []raft.NodeID
	addrs := map[raft.NodeID]string{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, addr, ok := strings.Cut(p, "=")
		if !ok {
			id, addr = p, p
		}
		ids = append(ids, raft.NodeID(id))
		addrs[raft.NodeID(id)] = addr
	}
	if len(ids) == 0 {
		return nil, nil, fmt.Errorf("--servers is required")
	}
	return ids, addrs, nil
}

func run() error {
	servers := flag.String("servers", os.Getenv("RAFTKV_SERVERS"), "client addresses: id=host:port,... (or env RAFTKV_SERVERS)")
	timeout := flag.Duration("timeout", 30*time.Second, "give up after this long")
	clientID := flag.String("client-id", "", "client id for exactly-once retries (default: random)")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage); flag.PrintDefaults() }
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("%s", usage)
	}
	ids, addrs, err := parseServers(*servers)
	if err != nil {
		return err
	}
	if *clientID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		*clientID = "cli-" + hex.EncodeToString(b)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if args[0] == "status" {
		return status(ctx, ids, addrs)
	}
	caller := kv.NewGRPCCaller(addrs)
	defer caller.Close()
	c := kv.NewClient(*clientID, ids, caller, clock.RealClock{})

	need := func(n int) error {
		if len(args)-1 < n {
			return fmt.Errorf("%s: missing arguments\n%s", args[0], usage)
		}
		return nil
	}
	switch args[0] {
	case "get":
		if err := need(1); err != nil {
			return err
		}
		v, err := c.Get(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Println(v)
	case "put":
		if err := need(2); err != nil {
			return err
		}
		if err := c.Put(ctx, args[1], args[2]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "append":
		if err := need(2); err != nil {
			return err
		}
		if err := c.Append(ctx, args[1], args[2]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "add-server":
		if err := need(2); err != nil {
			return err
		}
		if len(args) > 3 {
			caller.SetAddr(raft.NodeID(args[1]), args[3])
		}
		if err := c.AddServer(ctx, raft.NodeID(args[1]), args[2]); err != nil {
			return err
		}
		fmt.Println("OK")
	case "remove-server":
		if err := need(1); err != nil {
			return err
		}
		if err := c.RemoveServer(ctx, raft.NodeID(args[1])); err != nil {
			return err
		}
		fmt.Println("OK")
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
	return nil
}

// status asks every server for its admin status and prints one line each.
func status(ctx context.Context, ids []raft.NodeID, addrs map[raft.NodeID]string) error {
	sorted := append([]raft.NodeID(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var cfg []string
	for _, id := range sorted {
		conn, err := grpc.NewClient(addrs[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("%-6s unreachable: %v\n", id, err)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r, err := adminpb.NewAdminClient(conn).Status(cctx, &adminpb.StatusRequest{})
		cancel()
		conn.Close()
		if err != nil {
			fmt.Printf("%-6s unreachable\n", id)
			continue
		}
		fmt.Printf("%-6s role=%-9s term=%d commit=%d applied=%d leader=%s\n",
			r.Id, r.Role, r.Term, r.CommitIndex, r.LastApplied, r.LeaderId)
		if r.Role == "leader" {
			cfg = r.Config
		}
	}
	if len(cfg) > 0 {
		fmt.Println("config (from leader):")
		for _, m := range cfg {
			fmt.Println("  " + m)
		}
	}
	return nil
}
