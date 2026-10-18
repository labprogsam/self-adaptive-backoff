package orchestrator

import (
	"testing"
	"time"

	"self-adaptive-backoff/internal/apisim"
	"self-adaptive-backoff/internal/config"
)

func TestParseWindows_UsesRecordedStartsAndStopsAtEnd(t *testing.T) {
	base := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	at := func(ms int) string { return base.Add(time.Duration(ms) * time.Millisecond).Format(time.RFC3339Nano) }
	// trocas registradas pelo apisim; a 3ª chegou 1,2s "atrasada" no relógio de
	// parede (ajuste de relógio) — o cronograma tem que seguir o registrado
	data := []byte("0,healthy," + at(0) + "\n" +
		"1,outage," + at(2000) + "\n" +
		"2,healthy," + at(5200) + "\n" +
		"3,outage," + at(7200) + "\n" +
		"4,healthy," + at(9200) + "\n")

	got, err := parseWindows(data, 2*time.Second, 2*time.Second, base.Add(8*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		kind       string
		start, end int
	}{
		{"healthy", 0, 2000},
		{"outage", 2000, 5200},
		{"healthy", 5200, 7200},
		{"outage", 7200, 9200}, // última antes do fim (8s): termina em início + duração
	}
	if len(got) != len(want) {
		t.Fatalf("esperado %d janelas, obtido %d: %+v", len(want), len(got), got)
	}
	for i, w := range want {
		ws, we := base.Add(time.Duration(w.start)*time.Millisecond), base.Add(time.Duration(w.end)*time.Millisecond)
		if got[i].Index != i || got[i].Kind != w.kind || !got[i].Start.Equal(ws) || !got[i].End.Equal(we) {
			t.Fatalf("janela %d: esperado %s [%dms, %dms), obtido %+v", i, w.kind, w.start, w.end, got[i])
		}
	}
}

func TestFailureWindowRows_EmptyForUniformPattern(t *testing.T) {
	r := RunResult{
		RunID:       "x",
		FinishedAt:  time.Now(),
		Apisim:      &apisim.Info{StartedAt: time.Now().Add(-10 * time.Second), Pattern: "uniform", HealthyWindowMs: 2000, OutageWindowMs: 2000},
		WindowsFile: "inexistente.windows",
	}
	if rows, err := r.failureWindowRows(); err != nil || len(rows) != 0 {
		t.Fatalf("padrão uniforme não tem janelas de falha, obtido %d linhas", len(rows))
	}
}

func TestRunResultRow_MatchesHeaderAndRecordsFactorLevels(t *testing.T) {
	cfg := config.LoadFrom(envLookup([]string{"BACKOFF_STRATEGY=aimd", "MAX_RETRIES=5", "PRODUCER_RATE=2.5"}))
	r := RunResult{
		RunID: "aimd_b30_outage_rep01", RoundName: "aimd_b30_outage", Repetition: 1, Status: "ok",
		StartedAt: time.Now(), FinishedAt: time.Now(), Messages: 1000,
		Consumer: &cfg, Producer: &cfg,
		Apisim: &apisim.Info{StartedAt: time.Now(), Pattern: "outage", ErrorRate: 0.3},
	}

	row := r.row()
	if len(row) != len(summaryHeader) {
		t.Fatalf("linha com %d colunas, cabeçalho com %d", len(row), len(summaryHeader))
	}
	col := map[string]string{}
	for i, h := range summaryHeader {
		col[h] = row[i]
	}
	checks := map[string]string{
		"factor_a_strategy":      "aimd",
		"factor_b_error_rate":    "0.3",
		"factor_c_pattern":       "outage",
		"factor_d_producer_rate": "2.5",
		"factor_e_max_retries":   "5",
		"base_backoff_ms":        "100", // default, não definido no ambiente
	}
	for k, want := range checks {
		if col[k] != want {
			t.Errorf("%s: esperado %q, obtido %q", k, want, col[k])
		}
	}

	// execução que falhou antes de ter configuração: linha continua com o número certo de colunas
	if got := len((RunResult{RunID: "falhou"}).row()); got != len(summaryHeader) {
		t.Fatalf("linha sem configuração com %d colunas, cabeçalho com %d", got, len(summaryHeader))
	}
}
