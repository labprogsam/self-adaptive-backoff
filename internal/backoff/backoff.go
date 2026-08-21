package backoff

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"
)

type Options struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

func Retry[T any](opts Options, fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		if attempt == opts.MaxRetries {
			break
		}

		wait := delay(opts.BaseDelay, opts.MaxDelay, attempt)
		log.Printf("tentativa %d/%d falhou: %v | aguardando %s antes de tentar novamente",
			attempt+1, opts.MaxRetries+1, err, wait)
		time.Sleep(wait)
	}

	return zero, fmt.Errorf("todas as %d tentativas falharam, último erro: %w", opts.MaxRetries+1, lastErr)
}

func delay(base, maxDelay time.Duration, attempt int) time.Duration {
	d := float64(base) * math.Pow(2, float64(attempt))
	if d > float64(maxDelay) {
		d = float64(maxDelay)
	}
	jitter := d * 0.2 * rand.Float64()
	return time.Duration(d + jitter)
}
