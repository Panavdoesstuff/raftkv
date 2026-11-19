package clock

import (
	"testing"
	"time"
)

func recv(ch <-chan time.Time) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestFakeTimerFiresOnlyAfterDeadline(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(100 * time.Millisecond)
	f.Advance(99 * time.Millisecond)
	if recv(tm.C()) {
		t.Fatal("fired early")
	}
	f.Advance(1 * time.Millisecond)
	if !recv(tm.C()) {
		t.Fatal("did not fire at deadline")
	}
	f.Advance(time.Second)
	if recv(tm.C()) {
		t.Fatal("one-shot timer fired twice")
	}
}

func TestFakeTimerStopAndReset(t *testing.T) {
	f := NewFake()
	tm := f.NewTimer(50 * time.Millisecond)
	if !tm.Stop() {
		t.Fatal("Stop on active timer should report true")
	}
	f.Advance(time.Second)
	if recv(tm.C()) {
		t.Fatal("stopped timer fired")
	}
	tm.Reset(10 * time.Millisecond)
	f.Advance(10 * time.Millisecond)
	if !recv(tm.C()) {
		t.Fatal("reset timer did not fire")
	}
}

func TestFakeTickerFiresEachPeriod(t *testing.T) {
	f := NewFake()
	tk := f.NewTicker(50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		f.Advance(50 * time.Millisecond)
		if !recv(tk.C()) {
			t.Fatalf("tick %d missing", i)
		}
	}
	tk.Stop()
	f.Advance(time.Second)
	if recv(tk.C()) {
		t.Fatal("stopped ticker fired")
	}
}

func TestFakeNowAdvancesAndFiresInOrder(t *testing.T) {
	f := NewFake()
	start := f.Now()
	a := f.NewTimer(30 * time.Millisecond)
	b := f.NewTimer(10 * time.Millisecond)
	f.Advance(time.Second)
	tb, ta := <-b.C(), <-a.C()
	if !tb.Before(ta) {
		t.Fatalf("timer b (%v) should fire before a (%v)", tb, ta)
	}
	if got := f.Now().Sub(start); got != time.Second {
		t.Fatalf("Now advanced %v, want 1s", got)
	}
}

func TestBlockUntil(t *testing.T) {
	f := NewFake()
	done := make(chan struct{})
	go func() { f.BlockUntil(2); close(done) }()
	f.NewTimer(time.Second)
	select {
	case <-done:
		t.Fatal("returned with only one timer")
	case <-time.After(20 * time.Millisecond):
	}
	f.NewTicker(time.Second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BlockUntil did not return")
	}
}

func TestRealClockTimer(t *testing.T) {
	var c Clock = RealClock{}
	tm := c.NewTimer(5 * time.Millisecond)
	select {
	case <-tm.C():
	case <-time.After(time.Second):
		t.Fatal("real timer did not fire")
	}
}

func TestSkewedClockDrift(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, time.Hour, 0.5)
	t0 := s.Now()
	if d := t0.Sub(f.Now()); d != time.Hour {
		t.Fatalf("offset = %v, want 1h", d)
	}
	f.Advance(10 * time.Second)
	if d := s.Now().Sub(t0); d != 5*time.Second {
		t.Fatalf("slow clock advanced %v for 10s of base time, want 5s", d)
	}
	s.SetRate(2)
	f.Advance(10 * time.Second)
	if d := s.Now().Sub(t0); d != 25*time.Second {
		t.Fatalf("after SetRate(2) advanced %v total, want 25s", d)
	}
	s.Jump(-5 * time.Second)
	if d := s.Now().Sub(t0); d != 20*time.Second {
		t.Fatalf("after Jump(-5s) advanced %v, want 20s", d)
	}
}

func TestSkewedTimerScaled(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 0.5) // slow: 100 ms local takes 200 ms of base time
	tm := s.NewTimer(100 * time.Millisecond)
	f.Advance(150 * time.Millisecond)
	if recv(tm.C()) {
		t.Fatal("slow-clock timer fired early")
	}
	f.Advance(50 * time.Millisecond)
	if !recv(tm.C()) {
		t.Fatal("slow-clock timer did not fire at 200 ms base time")
	}
}

func TestSkewedSetOffsetIsAbsolute(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 1)
	s.SetOffset(5 * time.Second)
	s.SetOffset(5 * time.Second) // idempotent: still 5 s, not 10 s
	if got := s.Now().Sub(f.Now()); got != 5*time.Second {
		t.Fatalf("skew after SetOffset(5s) twice = %v, want 5s", got)
	}
	f.Advance(3 * time.Second)
	s.SetOffset(-2 * time.Second) // replaces the earlier jump
	if got := s.Now().Sub(f.Now()); got != -2*time.Second {
		t.Fatalf("skew after SetOffset(-2s) = %v, want -2s", got)
	}
	s.SetOffset(0)
	if got := s.Now().Sub(f.Now()); got != 0 {
		t.Fatalf("SetOffset(0) left skew %v", got)
	}
	if got := s.Offset(); got != 0 {
		t.Fatalf("Offset() = %v, want 0", got)
	}
	s.Jump(time.Second) // the relative form accumulates
	s.Jump(time.Second)
	if got := s.Offset(); got != 2*time.Second {
		t.Fatalf("two Jump(1s) = %v, want 2s", got)
	}
}

// A rate change must reach timers that are already armed: a node that slows
// its clock mid-wait waits longer, one that speeds up waits less.
func TestSkewedRateChangeRescalesArmedTimer(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 1)
	tm := s.NewTimer(100 * time.Millisecond)
	f.Advance(40 * time.Millisecond) // 60 ms local remain
	s.SetRate(0.5)                   // 60 ms local = 120 ms base
	f.Advance(119 * time.Millisecond)
	if recv(tm.C()) {
		t.Fatal("armed timer fired early after the clock slowed")
	}
	f.Advance(time.Millisecond)
	if !recv(tm.C()) {
		t.Fatal("armed timer did not fire at the rescaled deadline")
	}

	tm2 := s.NewTimer(100 * time.Millisecond) // 200 ms base at rate 0.5
	f.Advance(100 * time.Millisecond)         // 50 ms local elapsed, 50 remain
	s.SetRate(2)                              // 50 ms local = 25 ms base
	f.Advance(24 * time.Millisecond)
	if recv(tm2.C()) {
		t.Fatal("timer fired early after the clock sped up")
	}
	f.Advance(time.Millisecond)
	if !recv(tm2.C()) {
		t.Fatal("armed timer did not fire sooner after the clock sped up")
	}
}

func TestSkewedRateChangeLeavesStoppedAndFiredTimersAlone(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 1)
	stopped := s.NewTimer(100 * time.Millisecond)
	stopped.Stop()
	fired := s.NewTimer(10 * time.Millisecond)
	f.Advance(20 * time.Millisecond)
	<-fired.C()
	s.SetRate(0.5)
	f.Advance(time.Second)
	if recv(stopped.C()) {
		t.Fatal("stopped timer was revived by SetRate")
	}
	if recv(fired.C()) {
		t.Fatal("fired timer was re-armed by SetRate")
	}
	// A Reset after a rate change uses the current rate.
	fired.Reset(10 * time.Millisecond)
	f.Advance(19 * time.Millisecond)
	if recv(fired.C()) {
		t.Fatal("Reset ignored the rate")
	}
	f.Advance(time.Millisecond)
	if !recv(fired.C()) {
		t.Fatal("Reset timer did not fire")
	}
}

func TestSkewedRateChangeRescalesTicker(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 1)
	tk := s.NewTicker(10 * time.Millisecond)
	f.Advance(10 * time.Millisecond)
	if !recv(tk.C()) {
		t.Fatal("no tick at rate 1")
	}
	s.SetRate(0.1) // 10 ms local = 100 ms base
	f.Advance(99 * time.Millisecond)
	if recv(tk.C()) {
		t.Fatal("ticker kept its old period after the clock slowed")
	}
	f.Advance(time.Millisecond)
	if !recv(tk.C()) {
		t.Fatal("no tick at the rescaled period")
	}
	s.SetRate(5) // 10 ms local = 2 ms base
	f.Advance(2 * time.Millisecond)
	if !recv(tk.C()) {
		t.Fatal("ticker did not speed up")
	}
	tk.Stop()
	s.SetRate(1)
	f.Advance(time.Second)
	if recv(tk.C()) {
		t.Fatal("stopped ticker revived by SetRate")
	}
}

// Heartbeat, election and lease logic compare Now() with stored local
// deadlines, so what matters is that Now() runs at the new rate immediately.
func TestSkewedNowContinuousAcrossRateChange(t *testing.T) {
	f := NewFake()
	s := NewSkewed(f, 0, 1)
	f.Advance(time.Second)
	before := s.Now()
	s.SetRate(0.05)
	if got := s.Now(); got.Before(before) || got.Sub(before) > time.Millisecond {
		t.Fatalf("reading jumped by %v on SetRate", got.Sub(before))
	}
	f.Advance(10 * time.Second)
	if d := s.Now().Sub(before); d != 500*time.Millisecond {
		t.Fatalf("10 s of base time at rate 0.05 = %v local, want 500ms", d)
	}
}
