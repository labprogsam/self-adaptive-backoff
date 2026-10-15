package backoff_test

import (
	"testing"
	"time"

	"self-adaptive-backoff/internal/backoff"
)

func TestAIMD_IncreasesAdditivelyOnFailure(t *testing.T) {
	a := backoff.NewAIMD(100*time.Millisecond, 50*time.Millisecond, 0.5, 10*time.Millisecond, time.Second)

	a.Observe(false)
	if got := a.NextDelay(0); got != 150*time.Millisecond {
		t.Fatalf("após 1 falha: esperado 150ms, obtido %s", got)
	}

	a.Observe(false)
	if got := a.NextDelay(0); got != 200*time.Millisecond {
		t.Fatalf("após 2 falhas: esperado 200ms, obtido %s", got)
	}
}

func TestAIMD_DecreasesMultiplicativelyOnSuccess(t *testing.T) {
	a := backoff.NewAIMD(200*time.Millisecond, 50*time.Millisecond, 0.5, 10*time.Millisecond, time.Second)

	a.Observe(true)
	if got := a.NextDelay(0); got != 100*time.Millisecond {
		t.Fatalf("após 1 sucesso: esperado 100ms, obtido %s", got)
	}
}

func TestAIMD_ClampsToMinAndMax(t *testing.T) {
	a := backoff.NewAIMD(100*time.Millisecond, 500*time.Millisecond, 0.1, 50*time.Millisecond, 300*time.Millisecond)

	a.Observe(false) // 100 + 500 = 600, deveria ser limitado a 300
	if got := a.NextDelay(0); got != 300*time.Millisecond {
		t.Fatalf("esperado teto de 300ms, obtido %s", got)
	}

	// várias quedas multiplicativas devem parar no piso, não em zero
	for i := 0; i < 20; i++ {
		a.Observe(true)
	}
	if got := a.NextDelay(0); got != 50*time.Millisecond {
		t.Fatalf("esperado piso de 50ms, obtido %s", got)
	}
}
