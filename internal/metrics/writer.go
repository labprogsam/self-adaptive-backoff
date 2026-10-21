// Package metrics grava, em CSV, um registro por tentativa de processamento
// de mensagem — o schema de saída "por tentativa" descrito em
// Design_experimental.md, Seção 8. É a fonte de dados usada para calcular as
// métricas da Seção 3 (taxa de sucesso, tempo de espera induzido pelo
// backoff, número de tentativas, sobrecarga de chamadas, percentual de
// descarte) fora do processo, em análise posterior.
package metrics

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var header = []string{
	"run_id",
	"message_id",
	"attempt_number",
	"timestamp",
	"duration_ms",
	"wait_after_ms",
	"attempt_result",
	"final_result",
}

// AttemptRecord é uma linha do schema por tentativa. FinalResult só é
// preenchido na última tentativa de cada mensagem (R1/R2/R3); nas
// intermediárias fica vazio.
type AttemptRecord struct {
	RunID         string
	MessageID     string
	AttemptNumber int
	Timestamp     time.Time
	Duration      time.Duration
	WaitAfter     time.Duration
	AttemptResult string // sucesso | timeout | erro_comunicacao | erro_dado
	FinalResult   string // "" | R1 | R2 | R3
}

// Writer grava AttemptRecord em um arquivo CSV, criando o diretório e o
// cabeçalho se o arquivo ainda não existir. Safe para uso concorrente,
// embora o consumidor atual (Qos=1, um único goroutine de processamento)
// não precise disso hoje — mantido por segurança caso a concorrência mude.
type Writer struct {
	mu   sync.Mutex
	file *os.File
	csv  *csv.Writer
}

func NewWriter(path string) (*Writer, error) {
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("erro ao criar diretório %q: %w", dir, err)
		}
	}

	_, statErr := os.Stat(path)
	exists := statErr == nil

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir %q: %w", path, err)
	}

	w := &Writer{file: f, csv: csv.NewWriter(f)}
	if !exists {
		if err := w.csv.Write(header); err != nil {
			f.Close()
			return nil, fmt.Errorf("erro ao escrever cabeçalho: %w", err)
		}
		w.csv.Flush()
	}
	return w, nil
}

func (w *Writer) WriteAttempt(r AttemptRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	row := []string{
		r.RunID,
		r.MessageID,
		strconv.Itoa(r.AttemptNumber),
		r.Timestamp.Format(time.RFC3339Nano),
		strconv.FormatInt(r.Duration.Milliseconds(), 10),
		strconv.FormatInt(r.WaitAfter.Milliseconds(), 10),
		r.AttemptResult,
		r.FinalResult,
	}
	if err := w.csv.Write(row); err != nil {
		return err
	}
	w.csv.Flush()
	return w.csv.Error()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.csv.Flush()
	return w.file.Close()
}
