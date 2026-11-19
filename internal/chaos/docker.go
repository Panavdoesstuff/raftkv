package chaos

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/panavdoesstuff/raftkv/internal/clock"
	"github.com/panavdoesstuff/raftkv/internal/kv"
	"github.com/panavdoesstuff/raftkv/internal/raft"
	"github.com/panavdoesstuff/raftkv/internal/transport"
	"github.com/panavdoesstuff/raftkv/proto/adminpb"
)

// DockerOptions configure the docker target.
type DockerOptions struct {
	ComposeFile string // default deploy/docker-compose.3.yml
	// HostPortBase: node nK's KV/admin port is published on HostPortBase+K
	// (the compose files publish 8001, 8002, ...). Default 8000.
	HostPortBase int
	// KeepUp leaves the cluster running after Close (debugging).
	KeepUp bool
}

// DockerTarget drives the compose cluster in deploy/: faults go in through the
// admin RPC of each container (network rules, clock) and through the docker
// CLI (kill, start, pause, unpause). Time is the wall clock.
//
// The cluster is torn down (volumes included) and rebuilt for every run, so a
// run starts from an empty store, which the history checker needs.
type DockerTarget struct {
	o      DockerOptions
	mode   kv.ReadMode
	caller *kv.GRPCCaller

	mu     sync.Mutex
	nodes  []raft.NodeID
	down   map[raft.NodeID]bool
	frozen map[raft.NodeID]bool
	conns  map[raft.NodeID]*grpc.ClientConn
	joiner raft.NodeID // the one provisioned joiner, if any
	joined bool
}

// NewDockerTarget brings the cluster up and returns once every node answers.
func NewDockerTarget(o DockerOptions, nodes int, mode kv.ReadMode) (Target, error) {
	if o.ComposeFile == "" {
		o.ComposeFile = "deploy/docker-compose.3.yml"
	}
	if o.HostPortBase == 0 {
		o.HostPortBase = 8000
	}
	d := &DockerTarget{o: o, mode: mode, down: map[raft.NodeID]bool{}, frozen: map[raft.NodeID]bool{},
		conns: map[raft.NodeID]*grpc.ClientConn{}}
	addrs := map[raft.NodeID]string{}
	for i := 1; i <= nodes; i++ {
		id := raft.NodeID("n" + strconv.Itoa(i))
		d.nodes = append(d.nodes, id)
		addrs[id] = d.hostAddr(id)
	}
	d.caller = kv.NewGRPCCaller(addrs)
	d.compose("--profile", "join", "down", "-v", "--remove-orphans") // fresh start; errors mean nothing was up
	if out, err := d.compose("up", "-d", "--build"); err != nil {
		return nil, fmt.Errorf("compose up: %v\n%s", err, out)
	}
	deadline := time.Now().Add(60 * time.Second)
	for _, id := range d.nodes {
		for {
			if _, err := d.status(id); err == nil {
				break
			}
			if time.Now().After(deadline) {
				d.Close()
				return nil, fmt.Errorf("node %s did not come up", id)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	return d, nil
}

func (d *DockerTarget) hostAddr(id raft.NodeID) string {
	n, _ := strconv.Atoi(strings.TrimPrefix(string(id), "n"))
	return "127.0.0.1:" + strconv.Itoa(d.o.HostPortBase+n)
}

func (d *DockerTarget) compose(args ...string) (string, error) {
	full := append([]string{"compose", "-f", d.o.ComposeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = append(os.Environ(), "READ_MODE="+d.mode.String())
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func (d *DockerTarget) admin(id raft.NodeID) (adminpb.AdminClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c := d.conns[id]; c != nil {
		return adminpb.NewAdminClient(c), nil
	}
	c, err := grpc.NewClient(d.hostAddr(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	d.conns[id] = c
	return adminpb.NewAdminClient(c), nil
}

func (d *DockerTarget) status(id raft.NodeID) (*adminpb.StatusReply, error) {
	a, err := d.admin(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	return a.Status(ctx, &adminpb.StatusRequest{})
}

func (d *DockerTarget) Name() string       { return "docker" }
func (d *DockerTarget) Clock() clock.Clock { return clock.RealClock{} }
func (d *DockerTarget) Caller() kv.Caller  { return d.caller }

func (d *DockerTarget) Nodes() []raft.NodeID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]raft.NodeID(nil), d.nodes...)
}

func (d *DockerTarget) usable(id raft.NodeID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.down[id] && !d.frozen[id]
}

func (d *DockerTarget) Leader() (raft.NodeID, bool) {
	var best raft.NodeID
	var bestTerm uint64
	for _, id := range d.Nodes() {
		if !d.usable(id) {
			continue
		}
		if r, err := d.status(id); err == nil && r.Role == "leader" && r.Term >= bestTerm {
			best, bestTerm = id, r.Term
		}
	}
	return best, best != ""
}

// SetRules sends each node the rules whose From it is (rules with an empty
// From go to every node). Nodes that are down are skipped: a restarted
// container starts with no rules, which is what RecoverAll wants anyway.
func (d *DockerTarget) SetRules(clearAll bool, rules []transport.Rule) error {
	per := map[raft.NodeID][]*adminpb.NetRule{}
	for _, r := range rules {
		if r.From == "" {
			for _, id := range d.Nodes() {
				per[id] = append(per[id], r.ToProto())
			}
		} else {
			per[r.From] = append(per[r.From], r.ToProto())
		}
	}
	var firstErr error
	for _, id := range d.Nodes() {
		if !d.usable(id) {
			continue
		}
		if !clearAll && len(per[id]) == 0 {
			continue
		}
		a, err := d.admin(id)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err = a.SetNetRules(ctx, &adminpb.SetNetRulesRequest{ClearAll: clearAll, Rules: per[id]})
			cancel()
		}
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("%s: %w", id, err)
		}
	}
	return firstErr
}

func (d *DockerTarget) SetClock(id raft.NodeID, setOffset bool, offset time.Duration, rate float64) error {
	a, err := d.admin(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = a.SetClock(ctx, &adminpb.SetClockRequest{OffsetMs: offset.Milliseconds(), SetOffset: setOffset, DriftRate: rate})
	return err
}

func (d *DockerTarget) docker(verb string, id raft.NodeID) error {
	if out, err := d.compose(verb, string(id)); err != nil {
		return fmt.Errorf("docker compose %s %s: %v: %s", verb, id, err, strings.TrimSpace(out))
	}
	return nil
}

func (d *DockerTarget) Crash(id raft.NodeID) error {
	err := d.docker("kill", id)
	if err == nil {
		d.mu.Lock()
		d.down[id], d.frozen[id] = true, false
		d.mu.Unlock()
	}
	return err
}

func (d *DockerTarget) Restart(id raft.NodeID) error {
	err := d.docker("start", id)
	if err == nil {
		d.mu.Lock()
		d.down[id] = false
		d.mu.Unlock()
	}
	return err
}

func (d *DockerTarget) Pause(id raft.NodeID) error {
	err := d.docker("pause", id)
	if err == nil {
		d.mu.Lock()
		d.frozen[id] = true
		d.mu.Unlock()
	}
	return err
}

func (d *DockerTarget) Resume(id raft.NodeID) error {
	err := d.docker("unpause", id)
	if err == nil {
		d.mu.Lock()
		d.frozen[id] = false
		d.mu.Unlock()
	}
	return err
}

// AddNode starts the compose file's one joiner container (profile "join": n4
// for the 3-node file, n6 for the 5-node one). Only one join per run: after it
// is removed its data directory is stale, so a second add is unsupported.
func (d *DockerTarget) AddNode(id raft.NodeID) (string, error) {
	d.mu.Lock()
	if d.joined {
		d.mu.Unlock()
		return "", ErrUnsupported
	}
	want := raft.NodeID("n" + strconv.Itoa(len(d.nodes)+1))
	if id != want {
		d.mu.Unlock()
		return "", fmt.Errorf("%w: the compose joiner is %s, not %s", ErrUnsupported, want, id)
	}
	d.joined, d.joiner = true, id
	d.mu.Unlock()
	if out, err := d.compose("--profile", "join", "up", "-d", string(id)); err != nil {
		return "", fmt.Errorf("start joiner: %v: %s", err, out)
	}
	d.caller.SetAddr(id, d.hostAddr(id))
	d.mu.Lock()
	d.nodes = append(d.nodes, id)
	d.mu.Unlock()
	return string(id) + ":7000", nil
}

func (d *DockerTarget) RecoverAll() error {
	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	nodes := d.Nodes()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	for _, id := range nodes {
		d.mu.Lock()
		fr, dn := d.frozen[id], d.down[id]
		d.mu.Unlock()
		if fr {
			keep(d.Resume(id))
		}
		if dn {
			keep(d.Restart(id))
		}
	}
	// Wait for the restarted nodes to answer before talking to them.
	for _, id := range nodes {
		for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(200 * time.Millisecond) {
			if _, err := d.status(id); err == nil {
				break
			}
		}
		keep(d.SetClock(id, true, 0, 1))
	}
	keep(d.SetRules(true, nil))
	return firstErr
}

func (d *DockerTarget) Close() {
	d.caller.Close()
	d.mu.Lock()
	for _, c := range d.conns {
		c.Close()
	}
	d.mu.Unlock()
	if !d.o.KeepUp {
		d.compose("--profile", "join", "down", "-v", "--remove-orphans")
	}
}
