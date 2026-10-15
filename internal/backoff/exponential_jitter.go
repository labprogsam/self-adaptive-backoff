package backoff

import (
	"math"
	"math/rand"
	"time"
)

// ExponentialJitter representa o nível 4 do Fator A: backoff exponencial com
// jitter proporcional (20% do delay calculado) — a estratégia originalmente
// implementada no projeto antes da comparação entre estratégias; a fórmula é
// preservada sem alterações.
type ExponentialJitter struct {
	Base time.Duration
	Max  time.Duration
}

func NewExponentialJitter(base, max time.Duration) *ExponentialJitter {
	return &ExponentialJitter{Base: base, Max: max}
}

func (e *ExponentialJitter) Name() string { return "exponential_jitter" }

func (e *ExponentialJitter) NextDelay(attempt int) time.Duration {
	d := float64(e.Base) * math.Pow(2, float64(attempt))
	if d > float64(e.Max) {
		d = float64(e.Max)
	}
	jitter := d * 0.2 * rand.Float64()
	return time.Duration(d + jitter)
}

func (e *ExponentialJitter) Observe(success bool) {}
