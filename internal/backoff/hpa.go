package backoff

import (
	"sync"
	"time"
)

// HPA representa o nível 6 do Fator A: o algoritmo do Horizontal Pod
// Autoscaler (Kubernetes) adaptado de "réplicas" para "delay", usando a
// fórmula original sem ganho adicional:
//
//	delay_novo = delay_atual × (taxa_erro_observada / setpoint)
//
// A "taxa de erro observada" é estimada sobre uma janela observável das
// últimas WindowSize tentativas (Design_experimental.md, Seção 4.1). O
// recálculo acontece uma vez a cada janela completa — não a cada tentativa
// individual — espelhando o HPA real, que reavalia a métrica em um intervalo
// fixo (um "tick" do controlador), não a cada evento isolado. Recalcular a
// cada tentativa faria o ajuste compor repetidamente sobre a mesma leva de
// falhas antes que a janela tivesse "girado", levando a um crescimento muito
// mais agressivo do que o pretendido pela fórmula. Assim como o AIMD,
// mantém estado entre mensagens.
type HPA struct {
	mu sync.Mutex

	current time.Duration
	min     time.Duration
	max     time.Duration

	setpoint float64
	window   []bool // true = a tentativa naquela posição falhou
	size     int
	pos      int
}

func NewHPA(initial time.Duration, setpoint float64, windowSize int, min, max time.Duration) *HPA {
	if windowSize <= 0 {
		windowSize = 1
	}
	return &HPA{
		current:  initial,
		min:      min,
		max:      max,
		setpoint: setpoint,
		window:   make([]bool, windowSize),
		size:     windowSize,
	}
}

func (h *HPA) Name() string { return "hpa" }

// NextDelay ignora attempt, pelo mesmo motivo do AIMD: o delay reflete o
// estado observado entre mensagens, não a posição da tentativa atual.
func (h *HPA) NextDelay(attempt int) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current
}

func (h *HPA) Observe(success bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.window[h.pos] = !success
	h.pos++

	if h.pos < h.size {
		return // janela observável ainda não completou um ciclo
	}

	failures := 0
	for _, failed := range h.window {
		if failed {
			failures++
		}
	}
	observedRate := float64(failures) / float64(h.size)

	next := time.Duration(float64(h.current) * (observedRate / h.setpoint))
	if next < h.min {
		next = h.min
	}
	if next > h.max {
		next = h.max
	}
	h.current = next

	h.pos = 0 // inicia a próxima janela
}
