package orchestrator

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// Config controla uma rodada de orquestração.
type Config struct {
	PlanPath    string
	Parallelism int
	BinDir      string
	MetricsDir  string
	// LogDir é onde fica a saída de cada processo filho; vazio desativa os logs.
	LogDir       string
	DrainTimeout time.Duration
	// RepoRoot é o diretório a partir do qual os binários (cmd/consumer,
	// cmd/producer, cmd/apisim) são compilados — por padrão, o diretório
	// de trabalho atual (o orquestrador espera ser rodado da raiz do repo).
	RepoRoot string
}

func Run(cfg Config) error {
	if cfg.Parallelism <= 0 {
		return fmt.Errorf("parallelism deve ser > 0")
	}

	plan, err := LoadPlan(cfg.PlanPath)
	if err != nil {
		return err
	}
	executions := Expand(plan)

	for _, dir := range []string{cfg.BinDir, cfg.MetricsDir, cfg.LogDir} {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("erro ao criar diretório %q: %w", dir, err)
		}
	}

	log.Printf("orquestrador: compilando binários em %s...", cfg.BinDir)
	bins, err := buildBinaries(cfg.RepoRoot, cfg.BinDir)
	if err != nil {
		return err
	}

	log.Printf("orquestrador: %d execuções (%d rodadas × %d repetições), paralelismo=%d",
		len(executions), len(plan.Rounds), plan.Repetitions, cfg.Parallelism)

	summary, err := newSummaryWriter(filepath.Join(cfg.MetricsDir, "runs_summary.csv"))
	if err != nil {
		return err
	}
	defer summary.close()

	sem := make(chan struct{}, cfg.Parallelism)
	var wg sync.WaitGroup

	for _, ex := range executions {
		ex := ex
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			result := runExecution(cfg, bins, plan.SharedEnv, ex)
			if err := summary.write(result); err != nil {
				log.Printf("[%s] erro ao gravar resumo: %v", ex.RunID, err)
			}
			log.Printf("[%s] status=%s duração=%.1fs %s",
				ex.RunID, result.Status, result.FinishedAt.Sub(result.StartedAt).Seconds(), result.Detail)
		}()
	}

	wg.Wait()
	log.Printf("orquestrador: concluído. Resumo em %s", filepath.Join(cfg.MetricsDir, "runs_summary.csv"))
	return nil
}

type binaries struct {
	consumer string
	producer string
	apisim   string
}

func buildBinaries(repoRoot, binDir string) (binaries, error) {
	b := binaries{
		consumer: filepath.Join(binDir, "consumer"),
		producer: filepath.Join(binDir, "producer"),
		apisim:   filepath.Join(binDir, "apisim"),
	}

	targets := []struct {
		out string
		pkg string
	}{
		{b.consumer, "./cmd/consumer"},
		{b.producer, "./cmd/producer"},
		{b.apisim, "./cmd/apisim"},
	}

	for _, t := range targets {
		cmd := exec.Command("go", "build", "-o", t.out, t.pkg)
		cmd.Dir = repoRoot
		if out, err := cmd.CombinedOutput(); err != nil {
			return binaries{}, fmt.Errorf("erro ao compilar %s: %w\n%s", t.pkg, err, out)
		}
	}

	return b, nil
}

// runExecution roda uma única rodada (apisim + consumer + producer isolados)
// do início ao fim: sobe o apisim numa porta livre, sobe o consumer apontando
// para ela, publica as mensagens, espera a fila drenar (todas processadas) e
// então encerra os processos de vida longa. Sequencial por dentro — é
// seguro rodar várias runExecution concorrentes porque cada uma usa porta,
// fila e arquivo de métricas próprios.
func runExecution(cfg Config, bins binaries, sharedEnv map[string]string, ex Execution) RunResult {
	result := RunResult{
		RunID:      ex.RunID,
		RoundName:  ex.RoundName,
		Repetition: ex.Repetition,
		StartedAt:  time.Now(),
	}
	fail := func(status, format string, args ...any) RunResult {
		result.Status = status
		result.Detail = fmt.Sprintf(format, args...)
		result.FinishedAt = time.Now()
		return result
	}

	port, listener, err := freePort()
	if err != nil {
		return fail("error", "erro ao alocar porta: %v", err)
	}

	queueName := "tasks_" + ex.RunID
	// Um subdiretório por rodada, com um CSV por repetição dentro:
	// <MetricsDir>/<round_name>/<run_id>.csv
	roundDir := filepath.Join(cfg.MetricsDir, ex.RoundName)
	if err := os.MkdirAll(roundDir, 0o755); err != nil {
		listener.Close()
		return fail("error", "erro ao criar diretório %q: %v", roundDir, err)
	}
	metricsFile := filepath.Join(roundDir, ex.RunID+".csv")
	apiURL := fmt.Sprintf("http://127.0.0.1:%d/process", port)

	apisimEnv := buildEnv(sharedEnv, ex.ApisimEnv, map[string]string{
		"APISIM_ADDR": fmt.Sprintf(":%d", port),
	})
	listener.Close() // encolhe a janela de corrida: fecha só agora, logo antes do apisim subir
	apisimProc, err := startProcess(bins.apisim, cfg.LogDir, ex.RunID+"_apisim", apisimEnv)
	if err != nil {
		return fail("error", "erro ao iniciar apisim: %v", err)
	}
	defer apisimProc.stop(5 * time.Second)

	if err := waitPortOpen(port, 5*time.Second); err != nil {
		return fail("error", "apisim não respondeu: %v", err)
	}

	consumerEnv := buildEnv(sharedEnv, ex.ConsumerEnv, map[string]string{
		"RUN_ID":       ex.RunID,
		"QUEUE_NAME":   queueName,
		"API_URL":      apiURL,
		"METRICS_FILE": metricsFile,
	})
	consumerProc, err := startProcess(bins.consumer, cfg.LogDir, ex.RunID+"_consumer", consumerEnv)
	if err != nil {
		return fail("error", "erro ao iniciar consumer: %v", err)
	}
	defer consumerProc.stop(5 * time.Second)

	// pequena folga para o consumer conectar ao RabbitMQ e declarar a fila
	// antes do producer começar a publicar.
	time.Sleep(500 * time.Millisecond)

	producerEnvMap := mergeMaps(sharedEnv, ex.ProducerEnv, map[string]string{
		"RUN_ID":     ex.RunID,
		"QUEUE_NAME": queueName,
	})
	expectedCount, err := expectedMessageCount(producerEnvMap)
	if err != nil {
		return fail("error", "%v", err)
	}

	producerEnv := buildEnv(producerEnvMap)
	producerProc, err := startProcess(bins.producer, cfg.LogDir, ex.RunID+"_producer", producerEnv)
	if err != nil {
		return fail("error", "erro ao iniciar producer: %v", err)
	}
	if err := producerProc.cmd.Wait(); err != nil {
		producerProc.logFile.Close()
		return fail("error", "producer terminou com erro: %v", err)
	}
	producerProc.logFile.Close()

	if err := waitDrained(metricsFile, expectedCount, cfg.DrainTimeout); err != nil {
		return fail("timeout", "%v", err)
	}

	result.Status = "ok"
	result.Detail = fmt.Sprintf("%d mensagens processadas", expectedCount)
	result.FinishedAt = time.Now()
	return result
}

// expectedMessageCount determina quantas mensagens esta execução deve
// publicar, contando os itens do arquivo de tarefas que o producer vai ler
// (TASKS_FILE no ambiente efetivo do producer — plano ou, como em buildEnv,
// o ambiente do próprio orquestrador — ou tasks.json por padrão, mesmo
// default de internal/config). Cada execução isolada precisa desse número
// para detectar o fim do processamento.
func expectedMessageCount(env map[string]string) (int, error) {
	path := env["TASKS_FILE"]
	if path == "" {
		path = os.Getenv("TASKS_FILE")
	}
	if path == "" {
		path = "tasks.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("erro ao ler arquivo de tarefas %q: %w", path, err)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return 0, fmt.Errorf("arquivo de tarefas %q não é um array JSON válido: %w", path, err)
	}
	if len(items) == 0 {
		return 0, fmt.Errorf("arquivo de tarefas %q está vazio", path)
	}
	return len(items), nil
}

func mergeMaps(maps ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			merged[k] = v
		}
	}
	return merged
}
