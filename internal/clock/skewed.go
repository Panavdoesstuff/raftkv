package clock

import (
	"sync"
	"time"
)

// SkewedClock wraps another Clock and distorts it: a constant offset plus a
// drift rate. Now() reports
//
//	localAnchor + (base.Now() - baseAnchor) * rate
//
// so rate 1.0 is a perfect clock, rate < 1 runs slow, rate > 1 runs fast. In
// tests it wraps a FakeClock, so the distortion is deterministic. Timers and
// tickers are created on the base clock with durations scaled by 1/rate, so a
// node on a slow clock really does wait longer in base time for "50 ms".
//
// Only timers created after a SetRate use the new rate; existing ones keep
// their base-clock period. (Phase 9 may rescale them.) Safe for concurrent use.
type SkewedClock struct {
	base Clock

	mu          sync.Mutex
	baseAnchor  time.Time
	localAnchor time.Time
	rate        float64
}

// NewSkewed returns a clock reading base's time plus offset, running at rate.
// A non-positive rate is treated as 1.
func NewSkewed(base Clock, offset time.Duration, rate float64) *SkewedClock {
	if rate <= 0 {
		rate = 1
	}
	now := base.Now()
	return &SkewedClock{base: base, baseAnchor: now, localAnchor: now.Add(offset), rate: rate}
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

// SetRate changes the drift rate from now on, keeping the reading continuous.
func (s *SkewedClock) SetRate(rate float64) {
	if rate <= 0 {
		rate = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localAnchor, s.baseAnchor, s.rate = s.nowLocked(), s.base.Now(), rate
}

// SetOffset jumps the reading by d (positive: forward).
func (s *SkewedClock) SetOffset(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.localAnchor = s.nowLocked().Add(d)
	s.baseAnchor = s.base.Now()
}

func (s *SkewedClock) scale(d time.Duration) time.Duration {
	s.mu.Lock()
	r := s.rate
	s.mu.Unlock()
	return time.Duration(float64(d) / r)
}

func (s *SkewedClock) NewTimer(d time.Duration) Timer {
	return &skewedTimer{s: s, Timer: s.base.NewTimer(s.scale(d))}
}

func (s *SkewedClock) NewTicker(d time.Duration) Ticker { return s.base.NewTicker(s.scale(d)) }

type skewedTimer struct {
	s *SkewedClock
	Timer
}

func (t *skewedTimer) Reset(d time.Duration) bool { return t.Timer.Reset(t.s.scale(d)) }
