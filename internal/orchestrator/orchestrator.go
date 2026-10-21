package orchestrator

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"self-adaptive-backoff/internal/apisim"
	"self-adaptive-backoff/internal/config"
)

// Config controla uma rodada de orquestração.
type Config struct {
	PlanPath    string
	Concurrency int
	BinDir      string
	MetricsDir  string
	// LogDir é onde fica a saída de cada processo filho; vazio desativa os logs.
	LogDir       string
	DrainTimeout time.Duration
	// RepoRoot é o diretório a partir do qual os binários (cmd/consumer,
	// cmd/producer, cmd/apisim) são compilados — por padrão, o diretório
	// de trabalho atual (o orquestrador espera ser rodado da raiz do repo).
	RepoRoot string
	// Shuffle embaralha a ordem das execuções (Seção 8) usando Seed. A
	// ordem usada é gravada em <MetricsDir>/execution_order.csv.
	Shuffle bool
	Seed    int64
}

func Run(cfg Config) error {
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("concurrency deve ser > 0")
	}

	plan, err := LoadPlan(cfg.PlanPath)
	if err != nil {
		return err
	}
	executions := Expand(plan)

	// O CSV por tentativa é aberto para acrescentar (internal/metrics): rodar de
	// novo num diretório com resultados misturaria tentativas antigas e novas, e
	// waitDrained daria a execução por concluída na hora ao ver as mensagens
	// antigas. Por isso o diretório de saída precisa estar vazio.
	if err := ensureEmptyDir(cfg.MetricsDir); err != nil {
		return err
	}

	for _, dir := range []string{cfg.BinDir, cfg.MetricsDir, cfg.LogDir} {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("erro ao criar diretório %q: %w", dir, err)
		}
	}

	if cfg.Shuffle {
		Shuffle(executions, cfg.Seed)
		log.Printf("orquestrador: ordem das execuções embaralhada (seed=%d)", cfg.Seed)
	} else {
		log.Printf("orquestrador: ordem das execuções igual à do plano (sem embaralhar)")
	}
	if err := writeExecutionOrder(filepath.Join(cfg.MetricsDir, "execution_order.csv"), executions, cfg); err != nil {
		return err
	}

	log.Printf("orquestrador: compilando binários em %s...", cfg.BinDir)
	bins, err := buildBinaries(cfg.RepoRoot, cfg.BinDir)
	if err != nil {
		return err
	}

	log.Printf("orquestrador: %d execuções (%d rodadas × %d repetições), concorrência=%d",
		len(executions), len(plan.Rounds), plan.Repetitions, cfg.Concurrency)

	summary, err := newCSVWriter(filepath.Join(cfg.MetricsDir, "runs_summary.csv"), summaryHeader)
	if err != nil {
		return err
	}
	defer summary.close()
	windows, err := newCSVWriter(filepath.Join(cfg.MetricsDir, "failure_windows.csv"), failureWindowsHeader)
	if err != nil {
		return err
	}
	defer windows.close()

	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup

	for _, ex := range executions {
		ex := ex
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			result := runExecution(cfg, bins, plan.SharedEnv, ex)
			if err := summary.write(result.row()); err != nil {
				log.Printf("[%s] erro ao gravar resumo: %v", ex.RunID, err)
			}
			// runExecution já parou o apisim: o arquivo de janelas está completo.
			rows, err := result.failureWindowRows()
			if err == nil {
				err = windows.write(rows...)
			}
			if err != nil {
				log.Printf("[%s] erro ao gravar janelas de falha: %v", ex.RunID, err)
			}
			if result.WindowsFile != "" {
				_ = os.Remove(result.WindowsFile)
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
	apisimInfoFile := filepath.Join(roundDir, ex.RunID+".apisim.json")
	apisimWindowsFile := filepath.Join(roundDir, ex.RunID+".windows")
	apiURL := fmt.Sprintf("http://127.0.0.1:%d/process", port)

	// Ambiente de cada processo montado antes de iniciar qualquer um deles, para
	// que o registro por execução tenha a configuração efetiva mesmo se a
	// execução falhar no meio.
	consumerEnv := buildEnv(sharedEnv, ex.ConsumerEnv, map[string]string{
		"RUN_ID":       ex.RunID,
		"QUEUE_NAME":   queueName,
		"API_URL":      apiURL,
		"METRICS_FILE": metricsFile,
	})
	producerEnvMap := mergeMaps(sharedEnv, ex.ProducerEnv, map[string]string{
		"RUN_ID":     ex.RunID,
		"QUEUE_NAME": queueName,
	})
	producerEnv := buildEnv(producerEnvMap)
	consumerCfg := config.LoadFrom(envLookup(consumerEnv))
	producerCfg := config.LoadFrom(envLookup(producerEnv))
	result.Consumer, result.Producer = &consumerCfg, &producerCfg

	expectedCount, err := expectedMessageCount(producerEnvMap)
	if err != nil {
		listener.Close()
		return fail("error", "%v", err)
	}
	result.Messages = expectedCount

	apisimEnv := buildEnv(sharedEnv, ex.ApisimEnv, map[string]string{
		"APISIM_ADDR":         fmt.Sprintf(":%d", port),
		"APISIM_INFO_FILE":    apisimInfoFile,
		"APISIM_ID":           ex.RunID,
		"APISIM_WINDOWS_FILE": apisimWindowsFile,
	})
	result.WindowsFile = apisimWindowsFile
	listener.Close() // encolhe a janela de corrida: fecha só agora, logo antes do apisim subir
	apisimProc, err := startProcess(bins.apisim, cfg.LogDir, ex.RunID+"_apisim", apisimEnv)
	if err != nil {
		return fail("error", "erro ao iniciar apisim: %v", err)
	}
	defer apisimProc.stop(5 * time.Second)

	if err := waitApisimReady(port, ex.RunID, 5*time.Second); err != nil {
		return fail("error", "apisim não respondeu: %v", err)
	}
	// O apisim grava o arquivo antes de começar a escutar, então ele já existe
	// quando responde em /__health.
	info, err := readApisimInfo(apisimInfoFile)
	if err != nil {
		return fail("error", "%v", err)
	}
	result.Apisim = &info
	_ = os.Remove(apisimInfoFile)

	// Fila nova por execução: apaga qualquer resto de uma execução anterior com
	// o mesmo nome antes de começar, e apaga a fila ao terminar — o defer é
	// registrado antes do do consumer, então roda depois de o consumer parar.
	if err := deleteQueue(consumerCfg.AMQPUrl, queueName); err != nil {
		return fail("error", "%v", err)
	}
	defer func() {
		if err := deleteQueue(consumerCfg.AMQPUrl, queueName); err != nil {
			log.Printf("[%s] aviso: %v", ex.RunID, err)
		}
	}()

	consumerProc, err := startProcess(bins.consumer, cfg.LogDir, ex.RunID+"_consumer", consumerEnv)
	if err != nil {
		return fail("error", "erro ao iniciar consumer: %v", err)
	}
	defer consumerProc.stop(5 * time.Second)

	// pequena folga para o consumer conectar ao RabbitMQ e declarar a fila
	// antes do producer começar a publicar.
	time.Sleep(500 * time.Millisecond)

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

// writeExecutionOrder grava a ordem em que as execuções são iniciadas, junto
// com a semente usada, para que a aleatorização (Seção 8) fique registrada
// e possa ser reproduzida.
func writeExecutionOrder(path string, executions []Execution, cfg Config) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("erro ao criar %q: %w", path, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	seed := ""
	if cfg.Shuffle {
		seed = strconv.FormatInt(cfg.Seed, 10)
	}
	if err := w.Write([]string{"position", "run_id", "round_name", "repetition", "seed"}); err != nil {
		return err
	}
	for i, ex := range executions {
		row := []string{strconv.Itoa(i + 1), ex.RunID, ex.RoundName, strconv.Itoa(ex.Repetition), seed}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// readApisimInfo lê o arquivo de Info gravado pelo apisim (APISIM_INFO_FILE).
func readApisimInfo(path string) (apisim.Info, error) {
	var info apisim.Info
	data, err := os.ReadFile(path)
	if err != nil {
		return info, fmt.Errorf("erro ao ler informações do apisim %q: %w", path, err)
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, fmt.Errorf("informações do apisim %q inválidas: %w", path, err)
	}
	return info, nil
}

// envLookup transforma um ambiente no formato KEY=VALUE (o mesmo passado ao
// processo filho) numa função de consulta, para config.LoadFrom.
func envLookup(env []string) func(string) string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		if i := strings.IndexByte(e, '='); i >= 0 {
			m[e[:i]] = e[i+1:]
		}
	}
	return func(k string) string { return m[k] }
}

// ensureEmptyDir falha se dir já existe e tem algum conteúdo.
func ensureEmptyDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("erro ao ler diretório de saída %q: %w", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("diretório de saída %q já tem conteúdo (%d itens): use outro -metrics-dir ou remova o diretório antes de rodar", dir, len(entries))
	}
	return nil
}
