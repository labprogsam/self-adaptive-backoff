package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCSV(t *testing.T, dir, name string, rows [][]string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, row := range rows {
		for i, cell := range row {
			if i > 0 {
				f.WriteString(",")
			}
			f.WriteString(cell)
		}
		f.WriteString("\n")
	}
	return path
}

func TestCountFinalResults_CountsOnlyDistinctMessagesWithFinalResult(t *testing.T) {
	dir := t.TempDir()
	path := writeCSV(t, dir, "attempts.csv", [][]string{
		{"run_id", "message_id", "attempt_number", "timestamp", "duration_ms", "wait_after_ms", "attempt_result", "final_result"},
		{"r1", "m1", "1", "t", "1", "0", "sucesso", "R1"},
		{"r1", "m2", "1", "t", "1", "10", "erro_comunicacao", ""},
		{"r1", "m2", "2", "t", "1", "0", "sucesso", "R2"},
		{"r1", "m3", "1", "t", "1", "0", "erro_dado", "R3"},
	})

	n, err := countFinalResults(path)
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if n != 3 {
		t.Fatalf("esperado 3 mensagens finalizadas, obtido %d", n)
	}
}

func TestWaitDrained_TimesOutIfNeverReachesExpected(t *testing.T) {
	dir := t.TempDir()
	path := writeCSV(t, dir, "attempts.csv", [][]string{
		{"run_id", "message_id", "attempt_number", "timestamp", "duration_ms", "wait_after_ms", "attempt_result", "final_result"},
		{"r1", "m1", "1", "t", "1", "0", "sucesso", "R1"},
	})

	err := waitDrained(path, 5, 300*time.Millisecond)
	if err == nil {
		t.Fatal("esperado erro de timeout, obtido nil")
	}
}

func TestWaitDrained_SucceedsOnceFileIsUpdated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "attempts.csv")
	writeCSV(t, dir, "attempts.csv", [][]string{
		{"run_id", "message_id", "attempt_number", "timestamp", "duration_ms", "wait_after_ms", "attempt_result", "final_result"},
		{"r1", "m1", "1", "t", "1", "0", "sucesso", "R1"},
	})

	done := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		writeCSV(t, dir, "attempts.csv", [][]string{
			{"run_id", "message_id", "attempt_number", "timestamp", "duration_ms", "wait_after_ms", "attempt_result", "final_result"},
			{"r1", "m1", "1", "t", "1", "0", "sucesso", "R1"},
			{"r1", "m2", "1", "t", "1", "0", "sucesso", "R1"},
		})
		close(done)
	}()

	if err := waitDrained(path, 2, 2*time.Second); err != nil {
		t.Fatalf("esperado sucesso, obtido erro: %v", err)
	}
	<-done
}
