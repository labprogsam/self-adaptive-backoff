package backoff

import "time"

// Fixed representa o nível 2 do Fator A: delay constante entre tentativas.
type Fixed struct {
	Delay time.Duration
}

func NewFixed(delay time.Duration) *Fixed { return &Fixed{Delay: delay} }

func (f *Fixed) Name() string { return "fixed" }

func (f *Fixed) NextDelay(attempt int) time.Duration { return f.Delay }

func (f *Fixed) Observe(success bool) {}
