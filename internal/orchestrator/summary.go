package orchestrator

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

// RunResult é o desfecho de uma Execution — uma linha do resumo por rodada
// (não confundir com o CSV por tentativa de internal/metrics).
type RunResult struct {
	RunID      string
	RoundName  string
	Repetition int
	Status     string // ok | timeout | error
	Detail     string
	StartedAt  time.Time
	FinishedAt time.Time
}

var summaryHeader = []string{
	"run_id", "round_name", "repetition", "status", "detail",
	"started_at", "finished_at", "duration_s",
}

type summaryWriter struct {
	mu   sync.Mutex
	file *os.File
	csv  *csv.Writer
}

func newSummaryWriter(path string) (*summaryWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar resumo %q: %w", path, err)
	}
	w := &summaryWriter{file: f, csv: csv.NewWriter(f)}
	if err := w.csv.Write(summaryHeader); err != nil {
		f.Close()
		return nil, err
	}
	w.csv.Flush()
	return w, nil
}

func (w *summaryWriter) write(r RunResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	row := []string{
		r.RunID,
		r.RoundName,
		fmt.Sprintf("%d", r.Repetition),
		r.Status,
		r.Detail,
		r.StartedAt.Format(time.RFC3339),
		r.FinishedAt.Format(time.RFC3339),
		fmt.Sprintf("%.1f", r.FinishedAt.Sub(r.StartedAt).Seconds()),
	}
	if err := w.csv.Write(row); err != nil {
		return err
	}
	w.csv.Flush()
	return w.csv.Error()
}

func (w *summaryWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.csv.Flush()
	return w.file.Close()
}
