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
