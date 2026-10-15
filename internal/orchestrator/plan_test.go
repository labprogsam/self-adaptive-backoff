package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPlan_ValidatesRepetitionsAndRounds(t *testing.T) {
	dir := t.TempDir()

	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	if _, err := LoadPlan(write("no_rounds.json", `{"repetitions":1,"rounds":[]}`)); err == nil {
		t.Fatal("esperado erro para plano sem rounds")
	}

	if _, err := LoadPlan(write("no_reps.json", `{"repetitions":0,"rounds":[{"name":"a"}]}`)); err == nil {
		t.Fatal("esperado erro para repetitions <= 0")
	}

	dup := `{"repetitions":1,"rounds":[{"name":"a"},{"name":"a"}]}`
	if _, err := LoadPlan(write("dup.json", dup)); err == nil {
		t.Fatal("esperado erro para nomes de rodada duplicados")
	}

	valid := `{"repetitions":2,"rounds":[{"name":"a"},{"name":"b"}]}`
	p, err := LoadPlan(write("valid.json", valid))
	if err != nil {
		t.Fatalf("plano válido rejeitado: %v", err)
	}
	if len(p.Rounds) != 2 || p.Repetitions != 2 {
		t.Fatalf("plano carregado incorretamente: %+v", p)
	}
}

func TestExpand_GeneratesOneExecutionPerRoundAndRepetition(t *testing.T) {
	p := Plan{
		Repetitions: 3,
		Rounds: []Round{
			{Name: "a"},
			{Name: "b"},
		},
	}

	execs := Expand(p)
	if len(execs) != 6 {
		t.Fatalf("esperado 6 execuções (2 rodadas x 3 repetições), obtido %d", len(execs))
	}

	seen := map[string]bool{}
	for _, e := range execs {
		if seen[e.RunID] {
			t.Fatalf("run_id duplicado: %s", e.RunID)
		}
		seen[e.RunID] = true
	}

	if execs[0].RunID != "a_rep01" {
		t.Fatalf("esperado run_id \"a_rep01\", obtido %q", execs[0].RunID)
	}
}
