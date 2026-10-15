package backoff

import (
	"math"
	"time"
)

// Exponential representa o nível 3 do Fator A: backoff exponencial sem
// jitter — o delay dobra a cada tentativa, até o teto MaxDelay.
type Exponential struct {
	Base time.Duration
	Max  time.Duration
}

func NewExponential(base, max time.Duration) *Exponential {
	return &Exponential{Base: base, Max: max}
}

func (e *Exponential) Name() string { return "exponential" }

func (e *Exponential) NextDelay(attempt int) time.Duration {
	d := float64(e.Base) * math.Pow(2, float64(attempt))
	if d > float64(e.Max) {
		d = float64(e.Max)
	}
	return time.Duration(d)
}

func (e *Exponential) Observe(success bool) {}
