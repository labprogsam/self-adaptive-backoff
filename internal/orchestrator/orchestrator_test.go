package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureEmptyDir_RefusesDirectoryWithResults(t *testing.T) {
	dir := t.TempDir()

	if err := ensureEmptyDir(filepath.Join(dir, "inexistente")); err != nil {
		t.Fatalf("diretório inexistente deveria ser aceito: %v", err)
	}
	if err := ensureEmptyDir(dir); err != nil {
		t.Fatalf("diretório vazio deveria ser aceito: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "runs_summary.csv"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := ensureEmptyDir(dir); err == nil {
		t.Fatal("diretório com resultados deveria ser recusado")
	}
}
