package backoff

import "time"

// None representa o nível 1 do Fator A: retry imediato, sem espera — o
// tratamento de controle do experimento.
type None struct{}

func NewNone() *None { return &None{} }

func (n *None) Name() string { return "none" }

func (n *None) NextDelay(attempt int) time.Duration { return 0 }

func (n *None) Observe(success bool) {}
