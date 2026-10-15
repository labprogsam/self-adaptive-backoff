// Package backoff implementa as estratégias de retry comparadas no TCC
// (Design_experimental.md, Fator A) e o laço de retry genérico que as usa.
// O pacote é agnóstico de transporte: nenhuma estratégia ou o próprio Backoff
// dependem de net/http ou amqp091-go.
package backoff

import (
	"fmt"
	"time"
)

// Strategy decide quanto tempo esperar antes de uma nova tentativa, dado o
// histórico de resultados.
//
// As estratégias estáticas (None, Fixed, Exponential, ExponentialJitter) são
// stateless: NextDelay depende só do número da tentativa dentro da mensagem
// atual, e Observe é um no-op. As estratégias adaptativas (AIMD, HPA) mantêm
// estado entre mensagens dentro de Observe — nelas, o delay atual não é
// recalculado do zero a cada mensagem, mas ajustado incrementalmente pelo
// histórico de sucessos/falhas observado (ver Design_experimental.md, Seção 5).
type Strategy interface {
	// Name identifica a estratégia (usado em logs/métricas).
	Name() string
	// NextDelay retorna quanto esperar antes da tentativa número attempt+1
	// (attempt é 0-indexado: a primeira espera, após a tentativa inicial
	// falhar, é NextDelay(0)).
	NextDelay(attempt int) time.Duration
	// Observe é chamado após cada tentativa, com seu resultado (sucesso ou
	// falha), para que estratégias com estado possam se atualizar.
	// Estratégias stateless ignoram a chamada.
	Observe(success bool)
}

// Params agrupa os parâmetros possíveis de qualquer estratégia — cada
// construtor usa só o subconjunto relevante ao seu Fator A (Seção 4.1).
type Params struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// MinDelay é o piso do estado adaptativo do AIMD e do algoritmo do HPA
	// (não usado pelas estratégias estáticas).
	MinDelay time.Duration

	// AIMDIncrement e AIMDDecreaseFactor — nível 5 do Fator A.
	AIMDIncrement      time.Duration
	AIMDDecreaseFactor float64

	// HPASetpoint e HPAWindowSize — nível 6 do Fator A.
	HPASetpoint   float64
	HPAWindowSize int
}

// New constrói a estratégia identificada por name (os nomes correspondem aos
// níveis do Fator A em Design_experimental.md, Seção 5):
//
//	none               - sem backoff / retry imediato [controle]
//	fixed              - backoff fixo
//	exponential        - backoff exponencial sem jitter
//	exponential_jitter - backoff exponencial com jitter
//	aimd               - AIMD
//	hpa                - algoritmo do HPA
func New(name string, p Params) (Strategy, error) {
	switch name {
	case "none":
		return NewNone(), nil
	case "fixed":
		return NewFixed(p.BaseDelay), nil
	case "exponential":
		return NewExponential(p.BaseDelay, p.MaxDelay), nil
	case "exponential_jitter":
		return NewExponentialJitter(p.BaseDelay, p.MaxDelay), nil
	case "aimd":
		return NewAIMD(p.BaseDelay, p.AIMDIncrement, p.AIMDDecreaseFactor, p.MinDelay, p.MaxDelay), nil
	case "hpa":
		return NewHPA(p.BaseDelay, p.HPASetpoint, p.HPAWindowSize, p.MinDelay, p.MaxDelay), nil
	default:
		return nil, fmt.Errorf("estratégia de backoff desconhecida: %q", name)
	}
}
