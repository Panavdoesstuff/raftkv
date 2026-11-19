package clock

import (
	"sync"
	"time"
)

// SkewedClock wraps another Clock and distorts it: an offset plus a drift
// rate. Now() reports
//
//	localAnchor + (base.Now() - baseAnchor) * rate
//
// so rate 1.0 is a perfect clock, rate < 1 runs slow, rate > 1 runs fast.
//
// Offset semantics (absolute). The "skew" is Now() - base.Now(). SetOffset(d)
// makes the skew exactly d at the moment of the call, whatever it was before,
// so calling it twice with the same d is the same as once, and SetOffset(0)
// always removes a jump. With a rate other than 1 the skew keeps changing
// after the call (the clocks run at different speeds); d is the value at the
// instant of the call. Jump(d) is the relative form: it adds d to the current
// reading.
//
// Timers and tickers. Their durations are local time: a timer for 50 ms
// fires after 50 ms of this clock, i.e. 50/rate ms of base time. A rate change
// takes effect on timers and tickers that are already armed: SetRate rescales
// the remaining time of every armed timer and changes the period of every
// live ticker to d/newRate. Offset changes do not move armed timers (a timer
// measures elapsed time, like a monotonic timer); code that compares Now()
// against a stored deadline (Raft's election and heartbeat deadlines, the
// lease) does see the jump, which is what makes an offset fault bite.
//
// A SetRate racing with a timer that is about to fire can re-arm it for a
// near-zero remainder, so it may fire once more than expected; callers
// already treat timer events as hints and re-check state. Safe for concurrent
// use.
type SkewedClock struct {
	base Clock

	mu          sync.Mutex
	baseAnchor  time.Time
	localAnchor time.Time
	rate        float64
	timers      map[*skewedTimer]struct{}
	tickers     map[*skewedTicker]struct{}
}

// NewSkewed returns a clock reading base's time plus offset, running at rate.
// A non-positive rate is treated as 1.
func NewSkewed(base Clock, offset time.Duration, rate float64) *SkewedClock {
	if rate <= 0 {
		rate = 1
	}
	now := base.Now()
	return &SkewedClock{
		base: base, baseAnchor: now, localAnchor: now.Add(offset), rate: rate,
		timers: map[*skewedTimer]struct{}{}, tickers: map[*skewedTicker]struct{}{},
	}
}

func (s *SkewedClock) nowLocked() time.Time {
	elapsed := s.base.Now().Sub(s.baseAnchor)
	return s.localAnchor.Add(time.Duration(float64(elapsed) * s.rate))
}

func (s *SkewedClock) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowLocked()
}

// Rate returns the current drift rate.
func (s *SkewedClock) Rate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rate
}

// Offset returns the current skew, Now() - base.Now().
func (s *SkewedClock) Offset() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowLocked().Sub(s.base.Now())
}

// SetRate changes the drift rate from now on, keeping the reading continuous,
// and rescales every armed timer and live ticker to the new rate.
func (s *SkewedClock) SetRate(rate float64) {
	if rate <= 0 {
		rate = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.nowLocked()
	s.localAnchor, s.baseAnchor, s.rate = now, s.base.Now(), rate
	for t := range s.timers {
		if t.armed && t.deadline.After(now) {
			t.Timer.Reset(time.Duration(float64(t.deadline.Sub(now)) / rate))
		} else if !t.armed || t.deadline.Before(now.Add(-time.Minute)) {
			delete(s.timers, t) // stopped, or fired long ago: forget it
		}
	}
	for k := range s.tickers {
		k.Ticker.Reset(scaleBy(k.period, rate))
	}
}

// SetOffset makes the skew (Now() - base.Now()) exactly d at this instant.
func (s *SkewedClock) SetOffset(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.base.Now()
	s.localAnchor, s.baseAnchor = b.Add(d), b
}

// Jump moves the reading by d relative to where it is now (positive:
// forward).
func (s *SkewedClock) Jump(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localAnchor = s.nowLocked().Add(d)
	s.baseAnchor = s.base.Now()
}

func scaleBy(d time.Duration, rate float64) time.Duration {
	out := time.Duration(float64(d) / rate)
	if out < 1 {
		out = 1
	}
	return out
}

func (s *SkewedClock) NewTimer(d time.Duration) Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &skewedTimer{s: s, armed: true, deadline: s.nowLocked().Add(d)}
	t.Timer = s.base.NewTimer(scaleBy(d, s.rate))
	s.timers[t] = struct{}{}
	return t
}

func (s *SkewedClock) NewTicker(d time.Duration) Ticker {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := &skewedTicker{s: s, period: d}
	k.Ticker = s.base.NewTicker(scaleBy(d, s.rate))
	s.tickers[k] = struct{}{}
	return k
}

type skewedTimer struct {
	s *SkewedClock
	Timer
	armed    bool      // guarded by s.mu
	deadline time.Time // local time; guarded by s.mu
}

func (t *skewedTimer) Stop() bool {
	t.s.mu.Lock()
	t.armed = false
	delete(t.s.timers, t)
	t.s.mu.Unlock()
	return t.Timer.Stop()
}

func (t *skewedTimer) Reset(d time.Duration) bool {
	t.s.mu.Lock()
	t.armed = true
	t.deadline = t.s.nowLocked().Add(d)
	t.s.timers[t] = struct{}{}
	nd := scaleBy(d, t.s.rate)
	t.s.mu.Unlock()
	return t.Timer.Reset(nd)
}

type skewedTicker struct {
	s *SkewedClock
	Ticker
	period time.Duration // local
}

func (k *skewedTicker) Stop() {
	k.s.mu.Lock()
	delete(k.s.tickers, k)
	k.s.mu.Unlock()
	k.Ticker.Stop()
}

func (k *skewedTicker) Reset(d time.Duration) {
	k.s.mu.Lock()
	k.period = d
	nd := scaleBy(d, k.s.rate)
	k.s.tickers[k] = struct{}{}
	k.s.mu.Unlock()
	k.Ticker.Reset(nd)
}
