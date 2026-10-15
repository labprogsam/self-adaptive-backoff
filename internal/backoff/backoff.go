package backoff

import (
	"fmt"
	"time"
)

// Attempt descreve o resultado de uma tentativa individual — o registro "por
// tentativa" do schema de saída da Seção 8 do Design_experimental.md.
type Attempt struct {
	Number    int           // 1-indexado
	StartedAt time.Time     // início da chamada a fn()
	Duration  time.Duration // duração de fn() (não inclui a espera depois)
	Err       error         // nil se esta tentativa teve sucesso
	WaitAfter time.Duration // quanto se esperou depois desta tentativa (0 se não houve espera, isto é, se foi a última)
}

// Outcome agrupa o resultado final e o histórico de tentativas de uma
// chamada a Backoff — dá para derivar diretamente o resultado terminal da
// mensagem (R1/R2/R3, Design_experimental.md Seção 2):
//
//	len(Attempts) == 1 && Err == nil -> R1 (sucesso imediato)
//	len(Attempts) >  1 && Err == nil -> R2 (sucesso após retry)
//	Err != nil                       -> R3 (esgotamento)
type Outcome[T any] struct {
	Result   T
	Attempts []Attempt
	Err      error
}

// TotalWait soma o tempo gasto em espera de backoff entre tentativas —
// métrica 3.2 do Design_experimental.md (exclui o tempo de execução de fn()
// em si, isolando o custo imposto pela estratégia).
func (o Outcome[T]) TotalWait() time.Duration {
	var total time.Duration
	for _, a := range o.Attempts {
		total += a.WaitAfter
	}
	return total
}

// Backoff executa fn até ter sucesso ou esgotar maxRetries tentativas extras,
// usando strategy para decidir quanto esperar entre tentativas e para
// observar o resultado de cada uma. fn pode ser qualquer operação que falhe
// com erro — o pacote não depende de HTTP nem AMQP.
func Backoff[T any](strategy Strategy, maxRetries int, fn func() (T, error)) Outcome[T] {
	var outcome Outcome[T]

	for attempt := 0; attempt <= maxRetries; attempt++ {
		started := time.Now()
		result, err := fn()
		a := Attempt{
			Number:    attempt + 1,
			StartedAt: started,
			Duration:  time.Since(started),
			Err:       err,
		}

		strategy.Observe(err == nil)

		if err == nil {
			outcome.Attempts = append(outcome.Attempts, a)
			outcome.Result = result
			return outcome
		}

		if attempt == maxRetries {
			outcome.Attempts = append(outcome.Attempts, a)
			outcome.Err = fmt.Errorf("todas as %d tentativas falharam, último erro: %w", maxRetries+1, err)
			return outcome
		}

		wait := strategy.NextDelay(attempt)
		a.WaitAfter = wait
		outcome.Attempts = append(outcome.Attempts, a)
		time.Sleep(wait)
	}

	return outcome
}
