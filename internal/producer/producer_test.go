package producer

import (
	"testing"
	"time"
)

func TestNextInterval_ConstantMatchesRate(t *testing.T) {
	got := nextInterval(10, false) // 10 msgs/s -> 100ms
	want := 100 * time.Millisecond
	if got != want {
		t.Fatalf("esperado %s, obtido %s", want, got)
	}
}

func TestNextInterval_PoissonAveragesToRate(t *testing.T) {
	const rate = 50.0 // msgs/s -> média esperada de 20ms
	const n = 20000

	var total time.Duration
	for i := 0; i < n; i++ {
		total += nextInterval(rate, true)
	}
	mean := total / time.Duration(n)

	want := 20 * time.Millisecond
	tolerance := 2 * time.Millisecond
	if diff := mean - want; diff > tolerance || diff < -tolerance {
		t.Fatalf("média do intervalo Poisson = %s, esperado próximo de %s (±%s)", mean, want, tolerance)
	}
}
