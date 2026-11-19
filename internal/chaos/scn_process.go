package chaos

import (
	"fmt"
	"time"
)

// crashRestart kills one node every 3 s and restarts it 0.8-1.8 s later; one
// node down at a time, so a majority always survives.
func crashRestart(e *Env) {
	for base := time.Duration(0); ; base += 3 * time.Second {
		if !e.WaitUntil(base + time.Second) {
			return
		}
		crashOnce(e)
		if !e.WaitUntil(base + time.Second + e.Between(800*time.Millisecond, 1800*time.Millisecond)) {
			restartCrashed(e)
			return
		}
		restartCrashed(e)
	}
}

func crashOnce(e *Env) {
	spec := "random"
	if e.Rng.Intn(3) == 0 {
		spec = "leader"
	}
	pick := e.Pick(e.IDs()) // always drawn, so the RNG stream ignores the live leader
	victim := pick
	if spec == "leader" {
		if l, ok := e.Leader(); ok {
			victim = l
		}
	}
	e.crashedNode = victim
	e.Log("crash", fmt.Sprintf("%s#%s", spec, pick), string(victim), e.T.Crash(victim))
}

func restartCrashed(e *Env) {
	if v := e.crashedNode; v != "" {
		e.crashedNode = ""
		e.Log("restart", "last-crashed", string(v), e.T.Restart(v))
	}
}

// pauseLeader freezes the leader for 450-900 ms (the election timeout is
// 150-300 ms) every 4 s.
func pauseLeader(e *Env) {
	for base := time.Duration(0); ; base += 4 * time.Second {
		if !e.WaitUntil(base + time.Second) {
			return
		}
		hold := e.Between(450*time.Millisecond, 900*time.Millisecond)
		pauseOnce(e, hold)
		if !e.Wait(hold) {
			resumePaused(e)
			return
		}
		resumePaused(e)
	}
}

func pauseOnce(e *Env, hold time.Duration) {
	l, ok := e.Leader()
	if !ok {
		e.Log("pause", fmt.Sprintf("leader for=%dms", hold.Milliseconds()), "no leader", nil)
		return
	}
	e.pausedNode = l
	e.Log("pause", fmt.Sprintf("leader for=%dms", hold.Milliseconds()), string(l), e.T.Pause(l))
}

func resumePaused(e *Env) {
	if v := e.pausedNode; v != "" {
		e.pausedNode = ""
		e.Log("resume", "last-paused", string(v), e.T.Resume(v))
	}
}
