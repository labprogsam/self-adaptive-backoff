package backoff

import (
	"sync"
	"time"
)

// AIMD representa o nível 5 do Fator A: Additive Increase / Multiplicative
// Decrease. Diferente das estratégias estáticas, mantém um delay que
// persiste **entre mensagens** — cresce aditivamente a cada tentativa que
// falha e cai multiplicativamente a cada tentativa com sucesso
// (Design_experimental.md, Seção 4.1 e nota da Seção 5).
type AIMD struct {
	mu sync.Mutex

	current        time.Duration
	increment      time.Duration // Δ+ somado ao delay a cada falha
	decreaseFactor float64       // multiplicador aplicado ao delay a cada sucesso (0 < f < 1)
	min            time.Duration
	max            time.Duration
}

func NewAIMD(initial, increment time.Duration, decreaseFactor float64, min, max time.Duration) *AIMD {
	return &AIMD{
		current:        initial,
		increment:      increment,
		decreaseFactor: decreaseFactor,
		min:            min,
		max:            max,
	}
}

func (a *AIMD) Name() string { return "aimd" }

// NextDelay ignora attempt: o delay do AIMD não depende de quantas
// tentativas já foram feitas nesta mensagem, só do estado acumulado entre
// mensagens (atualizado em Observe).
func (a *AIMD) NextDelay(attempt int) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current
}

func (a *AIMD) Observe(success bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if success {
		a.current = time.Duration(float64(a.current) * a.decreaseFactor)
	} else {
		a.current += a.increment
	}

	if a.current < a.min {
		a.current = a.min
	}
	if a.current > a.max {
		a.current = a.max
	}
}
