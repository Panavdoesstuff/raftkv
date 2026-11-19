//go:build integration

// Package integration runs real cmd/server processes on localhost and checks
// the cluster the way an operator would: kill -9 the leader, restart a node,
// grow and shrink the cluster, and use the CLI. It needs the integration build
// tag so `make test` stays fast:
//
//	go test -tags integration -count=1 ./test/integration   (or make integration)
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

var serverBin, clientBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "raftkv-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	serverBin, clientBin = filepath.Join(dir, "server"), filepath.Join(dir, "client")
	for _, b := range [][2]string{{serverBin, "../../cmd/server"}, {clientBin, "../../cmd/client"}} {
		if out, err := exec.Command("go", "build", "-o", b[0], b[1]).CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", b[1], err, out)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	id                   string
	raftAddr, clientAddr string
	dataDir, logPath     string
	cmd                  *exec.Cmd
	done                 chan struct{}
	exitErr              error
}

type cluster struct {
	t     *testing.T
	root  string
	nodes map[string]*proc
	peers string // id=raftaddr,... of the initial voters
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{t: t, root: t.TempDir(), nodes: map[string]*proc{}}
	var peers []string
	for i := 1; i <= n; i++ {
		p := c.newProc(fmt.Sprintf("n%d", i))
		peers = append(peers, p.id+"="+p.raftAddr)
	}
	c.peers = strings.Join(peers, ",")
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) newProc(id string) *proc {
	p := &proc{
		id: id, raftAddr: freeAddr(c.t), clientAddr: freeAddr(c.t),
		dataDir: filepath.Join(c.root, id), logPath: filepath.Join(c.root, id+".log"),
	}
	c.nodes[id] = p
	return p
}

// start launches (or relaunches, on the same data dir) a node.
func (c *cluster) start(id string, join bool) {
	c.t.Helper()
	p := c.nodes[id]
	peers := c.peers
	if join {
		peers += "," + id + "=" + p.raftAddr // a joiner also lists itself: its own raft address
	}
	args := []string{"--id", id, "--peers", peers,
		"--raft-port", portOf(p.raftAddr), "--client-port", portOf(p.clientAddr),
		"--host", "127.0.0.1", "--data-dir", p.dataDir, "--read-mode", "readindex"}
	if join {
		args = append(args, "--join")
	}
	lf, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(serverBin, args...)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	p.cmd, p.done = cmd, make(chan struct{})
	go func() { p.exitErr = cmd.Wait(); lf.Close(); close(p.done) }()
}

func portOf(addr string) string { _, p, _ := net.SplitHostPort(addr); return p }

func (p *proc) running() bool {
	if p.cmd == nil {
		return false
	}
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (c *cluster) kill9(id string) {
	p := c.nodes[id]
	p.cmd.Process.Kill()
	<-p.done
}

func (c *cluster) shutdown() {
	for _, p := range c.nodes {
		if p.running() {
			p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				c.t.Errorf("%s did not exit on SIGTERM", p.id)
				p.cmd.Process.Kill()
				<-p.done
			}
		}
	}
	if c.t.Failed() {
		for _, p := range c.nodes {
			b, _ := os.ReadFile(p.logPath)
			if len(b) > 3000 {
				b = b[len(b)-3000:]
			}
			c.t.Logf("--- %s log (tail) ---\n%s", p.id, b)
		}
	}
}

func (c *cluster) clientAddrs() string {
	var parts []string
	for _, p := range c.nodes {
		if p.running() {
			parts = append(parts, p.id+"="+p.clientAddr)
		}
	}
	return strings.Join(parts, ",")
}

func (c *cluster) status(id string) (*adminpb.StatusReply, error) {
	conn, err := grpc.NewClient(c.nodes[id].clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return adminpb.NewAdminClient(conn).Status(ctx, &adminpb.StatusRequest{})
}

func (c *cluster) waitFor(what string, limit time.Duration, cond func() bool) {
	c.t.Helper()
	for end := time.Now().Add(limit); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if cond() {
			return
		}
	}
	c.t.Fatalf("timed out (%v) waiting for %s", limit, what)
}

// leader returns the running node that reports itself leader with the highest term.
func (c *cluster) leader() string {
	var best string
	var bestTerm uint64
	for id, p := range c.nodes {
		if !p.running() {
			continue
		}
		if r, err := c.status(id); err == nil && r.Role == "leader" && r.Term >= bestTerm {
			best, bestTerm = id, r.Term
		}
	}
	return best
}

func (c *cluster) waitLeader() string {
	c.t.Helper()
	var l string
	c.waitFor("a leader", 15*time.Second, func() bool { l = c.leader(); return l != "" })
	return l
}

// cli runs the client binary against the running nodes.
func (c *cluster) cli(args ...string) (string, error) {
	c.t.Helper()
	full := append([]string{"--servers", c.clientAddrs(), "--timeout", "40s"}, args...)
	var out bytes.Buffer
	cmd := exec.Command(clientBin, full...)
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

func (c *cluster) mustCLI(args ...string) string {
	c.t.Helper()
	out, err := c.cli(args...)
	if err != nil {
		c.t.Fatalf("client %v: %v\n%s", args, err, out)
	}
	return out
}

// kvClient is an in-process client over the running nodes.
func (c *cluster) kvClient(name string) (*kv.Client, func()) {
	addrs := map[raft.NodeID]string{}
	var ids []raft.NodeID
	for _, p := range c.nodes {
		if p.running() {
			ids = append(ids, raft.NodeID(p.id))
			addrs[raft.NodeID(p.id)] = p.clientAddr
		}
	}
	caller := kv.NewGRPCCaller(addrs)
	return kv.NewClient(name, ids, caller, clock.RealClock{}), caller.Close
}

func (c *cluster) catchUp(id string) {
	c.t.Helper()
	c.waitFor(id+" to catch up", 20*time.Second, func() bool {
		l := c.leader()
		if l == "" || l == id {
			return false
		}
		lr, err1 := c.status(l)
		nr, err2 := c.status(id)
		return err1 == nil && err2 == nil && nr.CommitIndex > 0 && nr.LastApplied >= lr.CommitIndex-1
	})
}

func voterCount(r *adminpb.StatusReply) int {
	n := 0
	for _, m := range r.Config {
		if strings.Contains(m, " voter") {
			n++
		}
	}
	return n
}

func TestProcessCluster(t *testing.T) {
	c := newCluster(t, 3)
	for _, id := range []string{"n1", "n2", "n3"} {
		c.start(id, false)
	}
	c.waitLeader()

	// 1. get/put/append through the CLI.
	c.mustCLI("put", "k", "v1")
	if got := c.mustCLI("get", "k"); got != "v1" {
		t.Fatalf("get k = %q, want v1", got)
	}
	c.mustCLI("append", "k", "+x")
	if got := c.mustCLI("get", "k"); got != "v1+x" {
		t.Fatalf("get k after append = %q, want v1+x", got)
	}
	if out := c.mustCLI("status"); !strings.Contains(out, "role=leader") || strings.Count(out, "role=") != 3 {
		t.Fatalf("status output unexpected:\n%s", out)
	}

	// 2. kill -9 the leader; writes resume on the survivors.
	old := c.waitLeader()
	c.kill9(old)
	cl, closeCl := c.kvClient("resume")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	start := time.Now()
	if err := cl.Put(ctx, "after-kill", "ok"); err != nil {
		t.Fatalf("write after killing leader %s: %v", old, err)
	}
	t.Logf("writes resumed %v after kill -9 of %s", time.Since(start).Round(time.Millisecond), old)
	if v, err := cl.Get(ctx, "k"); err != nil || v != "v1+x" {
		t.Fatalf("get k after failover = %q, %v", v, err)
	}
	cancel()
	closeCl()
	if nl := c.waitLeader(); nl == old {
		t.Fatalf("killed node %s still reported as leader", old)
	}

	// 3. restart the killed node on its old data dir; it catches up from disk.
	c.mustCLI("put", "while-down", "1")
	c.start(old, false)
	c.catchUp(old)
	if r, err := c.status(old); err != nil || r.Role == "leader" && r.Term == 0 {
		t.Fatalf("restarted node status: %+v %v", r, err)
	}

	// 4. add a 4th process as a joining non-voter, then remove it.
	n4 := c.newProc("n4")
	c.start("n4", true)
	c.mustCLI("add-server", "n4", n4.raftAddr, n4.clientAddr)
	c.waitFor("4 voters", 20*time.Second, func() bool {
		r, err := c.status(c.waitLeader())
		return err == nil && voterCount(r) == 4
	})
	c.catchUp("n4")
	c.mustCLI("put", "with-n4", "1")
	c.waitFor("n4 to apply the new write", 10*time.Second, func() bool {
		l, _ := c.status(c.waitLeader())
		r, err := c.status("n4")
		return err == nil && l != nil && r.LastApplied >= l.CommitIndex
	})
	c.mustCLI("remove-server", "n4")
	c.waitFor("3 voters", 20*time.Second, func() bool {
		r, err := c.status(c.waitLeader())
		return err == nil && voterCount(r) == 3
	})
	// n4 shuts down cleanly on SIGTERM (exit status 0).
	n4.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-n4.done:
		if n4.exitErr != nil {
			t.Fatalf("n4 exit after SIGTERM: %v", n4.exitErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("n4 did not exit on SIGTERM")
	}
	c.mustCLI("put", "after-remove", "1")
	for k, want := range map[string]string{"k": "v1+x", "after-kill": "ok", "while-down": "1", "with-n4": "1", "after-remove": "1"} {
		if got := c.mustCLI("get", k); got != want {
			t.Fatalf("get %s = %q, want %q", k, got, want)
		}
	}
}
