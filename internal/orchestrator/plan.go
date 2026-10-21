// Package orchestrator roda rodadas completas do experimento (apisim +
// consumer + producer, cada uma isolada) com concorrência limitada — a
// concorrência fica entre rodadas, nunca dentro de uma rodada (que continua
// sequencial, preservando Qos=1 e o estado compartilhado entre mensagens das
// estratégias adaptativas; ver Design_experimental.md, Seção 5.2).
package orchestrator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
)

// Plan descreve o conjunto de rodadas a executar. Cada combinação de fatores
// vira um Round; Repetitions é o número de execuções independentes por
// combinação (n, Seção 8 do design experimental).
type Plan struct {
	Repetitions int               `json:"repetitions"`
	SharedEnv   map[string]string `json:"shared_env"`
	Rounds      []Round           `json:"rounds"`
}

// Round é uma combinação de fatores: variáveis de ambiente para o consumer
// (Fator A e seus parâmetros, Fator E), para o apisim (Fatores B e C) e,
// quando necessário (ex.: triagem do Fator D), para o producer. ConsumerEnv,
// ApisimEnv e ProducerEnv são aplicados por cima de SharedEnv.
type Round struct {
	Name        string            `json:"name"`
	ConsumerEnv map[string]string `json:"consumer_env"`
	ApisimEnv   map[string]string `json:"apisim_env"`
	ProducerEnv map[string]string `json:"producer_env"`
}

func LoadPlan(path string) (Plan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, fmt.Errorf("erro ao ler plano %q: %w", path, err)
	}

	var p Plan
	if err := json.Unmarshal(data, &p); err != nil {
		return Plan{}, fmt.Errorf("plano %q não é um JSON válido: %w", path, err)
	}

	if p.Repetitions <= 0 {
		return Plan{}, fmt.Errorf("plano %q: repetitions deve ser > 0", path)
	}
	if len(p.Rounds) == 0 {
		return Plan{}, fmt.Errorf("plano %q: nenhuma rodada (rounds) definida", path)
	}
	seen := map[string]bool{}
	for _, r := range p.Rounds {
		if r.Name == "" {
			return Plan{}, fmt.Errorf("plano %q: rodada sem nome", path)
		}
		if seen[r.Name] {
			return Plan{}, fmt.Errorf("plano %q: nome de rodada duplicado: %q", path, r.Name)
		}
		seen[r.Name] = true
	}

	return p, nil
}

// Execution é uma rodada já expandida com sua repetição — a unidade real de
// trabalho do orquestrador (run_id único, isolado de todas as outras).
type Execution struct {
	RunID       string
	RoundName   string
	Repetition  int
	ConsumerEnv map[string]string
	ApisimEnv   map[string]string
	ProducerEnv map[string]string
}

// Expand gera todas as execuções (rodada × repetição) do plano, na ordem em
// que aparecem — a aleatorização da ordem de execução (Seção 8) é feita
// depois, por Shuffle.
func Expand(p Plan) []Execution {
	executions := make([]Execution, 0, len(p.Rounds)*p.Repetitions)
	for _, r := range p.Rounds {
		for rep := 1; rep <= p.Repetitions; rep++ {
			executions = append(executions, Execution{
				RunID:       fmt.Sprintf("%s_rep%02d", r.Name, rep),
				RoundName:   r.Name,
				Repetition:  rep,
				ConsumerEnv: r.ConsumerEnv,
				ApisimEnv:   r.ApisimEnv,
				ProducerEnv: r.ProducerEnv,
			})
		}
	}
	return executions
}

// Shuffle embaralha a ordem das execuções com uma semente explícita: a ordem
// aleatorizada entre combinações exigida pela Seção 8 do design
// experimental, para não confundir efeito de fator com efeito de
// tempo/ambiente. A mesma semente sempre produz a mesma ordem, o que permite
// registrar e reproduzir a ordem usada em cada experimento.
func Shuffle(executions []Execution, seed int64) {
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(executions), func(i, j int) {
		executions[i], executions[j] = executions[j], executions[i]
	})
}
