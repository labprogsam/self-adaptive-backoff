package orchestrator

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"time"
)

// waitDrained espera até que o CSV de métricas em path tenha um resultado
// final registrado (coluna final_result não vazia) para pelo menos expected
// mensagens distintas — sinal de que o consumidor já processou (ack ou nack)
// todas as mensagens publicadas nesta execução, sem precisar consultar a API
// de management do RabbitMQ.
func waitDrained(path string, expected int, timeout time.Duration) error {
	if expected <= 0 {
		return fmt.Errorf("número esperado de mensagens inválido: %d", expected)
	}

	deadline := time.Now().Add(timeout)
	for {
		n, err := countFinalResults(path)
		if err == nil && n >= expected {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("timeout esperando %d mensagens finalizadas (último erro ao ler %q: %w)", expected, path, err)
			}
			return fmt.Errorf("timeout esperando %d mensagens finalizadas (%d confirmadas em %q)", expected, n, path)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// countFinalResults conta quantas mensagens distintas já têm um resultado
// final (R1/R2/R3) registrado no CSV de métricas por tentativa (schema da
// Seção 8 do design experimental).
func countFinalResults(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	header, err := r.Read()
	if err != nil {
		return 0, err
	}

	msgIdx, finalIdx := -1, -1
	for i, col := range header {
		switch col {
		case "message_id":
			msgIdx = i
		case "final_result":
			finalIdx = i
		}
	}
	if msgIdx < 0 || finalIdx < 0 {
		return 0, fmt.Errorf("cabeçalho inesperado em %q: faltam colunas message_id/final_result", path)
	}

	seen := map[string]bool{}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		if len(row) <= finalIdx {
			continue
		}
		if row[finalIdx] != "" {
			seen[row[msgIdx]] = true
		}
	}

	return len(seen), nil
}
