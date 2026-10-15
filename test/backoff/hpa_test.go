package backoff_test

import (
	"testing"
	"time"

	"self-adaptive-backoff/internal/backoff"
)

func TestHPA_ScalesDelayByObservedOverSetpointRatio(t *testing.T) {
	// janela de 4, setpoint 25% (1 falha em 4 seria o "normal")
	h := backoff.NewHPA(100*time.Millisecond, 0.25, 4, 10*time.Millisecond, time.Second)

	// 4 falhas seguidas -> taxa observada = 100% -> razão = 0.25^-1 = 4x
	h.Observe(false)
	h.Observe(false)
	h.Observe(false)
	h.Observe(false)

	got := h.NextDelay(0)
	want := 400 * time.Millisecond
	if got != want {
		t.Fatalf("esperado %s (100ms x 4), obtido %s", want, got)
	}
}

func TestHPA_DecreasesWhenObservedBelowSetpoint(t *testing.T) {
	// janela de 4, setpoint 50%; só 1 falha em 4 -> taxa observada 25%, razão 0.5
	h := backoff.NewHPA(100*time.Millisecond, 0.5, 4, 10*time.Millisecond, time.Second)

	h.Observe(false)
	h.Observe(true)
	h.Observe(true)
	h.Observe(true)

	got := h.NextDelay(0)
	want := 50 * time.Millisecond
	if got != want {
		t.Fatalf("esperado %s (100ms x 0.5), obtido %s", want, got)
	}
}

func TestHPA_ClampsToMinAndMax(t *testing.T) {
	h := backoff.NewHPA(100*time.Millisecond, 0.1, 2, 20*time.Millisecond, 150*time.Millisecond)

	// 2 falhas em janela de 2 -> taxa observada 100%, razão 0.1^-1=10x -> 1000ms, deve travar em 150ms
	h.Observe(false)
	h.Observe(false)
	if got := h.NextDelay(0); got != 150*time.Millisecond {
		t.Fatalf("esperado teto de 150ms, obtido %s", got)
	}

	// 2 sucessos em janela de 2 -> taxa observada 0%, razão 0 -> delay cai para 0, deve travar no piso
	h.Observe(true)
	h.Observe(true)
	if got := h.NextDelay(0); got != 20*time.Millisecond {
		t.Fatalf("esperado piso de 20ms, obtido %s", got)
	}
}
