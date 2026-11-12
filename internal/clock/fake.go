package clock

import (
	"sort"
	"sync"
	"time"
)

// FakeClock only moves when Advance is called. Timers and tickers fire in
// deadline order as the clock passes them.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct{}
	cond   *sync.Cond // signalled when the set of active timers changes
}

type fakeTimer struct {
	c        *FakeClock
	ch       chan time.Time
	deadline time.Time
	period   time.Duration // >0 for tickers
	active   bool
}

// NewFake returns a FakeClock starting at an arbitrary fixed instant.
func NewFake() *FakeClock {
	f := &FakeClock{
		now:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		timers: map[*fakeTimer]struct{}{},
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) newTimer(d, period time.Duration) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{c: f, ch: make(chan time.Time, 1), deadline: f.now.Add(d), period: period, active: true}
	f.timers[t] = struct{}{}
	f.cond.Broadcast()
	return t
}

func (f *FakeClock) NewTimer(d time.Duration) Timer { return f.newTimer(d, 0) }

func (f *FakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive ticker interval")
	}
	return &fakeTicker{f.newTimer(d, d)}
}

// Advance moves time forward by d, firing every timer whose deadline is passed,
// in deadline order. Sends never block (channels are buffered, drops on full,
// like the real time package).
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	target := f.now.Add(d)
	for {
		var due []*fakeTimer
		for t := range f.timers {
			if !t.deadline.After(target) {
				due = append(due, t)
			}
		}
		if len(due) == 0 {
			break
		}
		sort.Slice(due, func(i, j int) bool { return due[i].deadline.Before(due[j].deadline) })
		t := due[0]
		f.now = t.deadline
		select {
		case t.ch <- f.now:
		default:
		}
		if t.period > 0 {
			t.deadline = t.deadline.Add(t.period)
		} else {
			t.active = false
			delete(f.timers, t)
		}
	}
	f.now = target
	f.cond.Broadcast()
}

// BlockUntil waits until at least n timers/tickers are active. Tests use it to
// know the code under test has armed its timers before calling Advance.
func (f *FakeClock) BlockUntil(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.timers) < n {
		f.cond.Wait()
	}
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.active = false
	delete(t.c.timers, t)
	t.c.cond.Broadcast()
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.active
	t.deadline = t.c.now.Add(d)
	t.active = true
	t.c.timers[t] = struct{}{}
	t.c.cond.Broadcast()
	return was
}

type fakeTicker struct{ t *fakeTimer }

func (k *fakeTicker) C() <-chan time.Time { return k.t.ch }
func (k *fakeTicker) Stop()               { k.t.Stop() }
