package orchestrator

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"self-adaptive-backoff/internal/apisim"
	"self-adaptive-backoff/internal/config"
)

// RunResult é o desfecho de uma Execution — uma linha do registro por
// execução (Design_experimental.md, Seção 8), gravado em runs_summary.csv.
// Não confundir com o CSV por tentativa de internal/metrics.
type RunResult struct {
	RunID      string
	RoundName  string
	Repetition int
	Status     string // ok | timeout | error
	Detail     string
	StartedAt  time.Time
	FinishedAt time.Time

	// Mensagens que a execução deveria processar (itens do arquivo de tarefas).
	Messages int
	// Configuração efetiva de cada processo, calculada a partir do mesmo
	// ambiente que ele recebeu (inclui os defaults). Nil se a execução falhou
	// antes de ela estar disponível.
	Consumer *config.Config
	Producer *config.Config
	// Configuração efetiva e instante de início do ciclo de falhas do apisim,
	// lidos do arquivo que o próprio apisim grava (APISIM_INFO_FILE).
	Apisim *apisim.Info
	// Arquivo em que o apisim registrou as trocas de janela
	// (APISIM_WINDOWS_FILE); lido depois que a execução termina.
	WindowsFile string
}

var summaryHeader = []string{
	"run_id", "round_name", "repetition", "status", "detail",
	"started_at", "finished_at", "duration_s", "messages",
	// níveis dos fatores A–E
	"factor_a_strategy", "factor_b_error_rate", "factor_c_pattern",
	"factor_d_producer_rate", "factor_e_max_retries",
	// parâmetros fixados e calibrados (Seção 5.2)
	"base_backoff_ms", "max_backoff_ms", "min_backoff_ms",
	"aimd_increment_ms", "aimd_decrease_factor", "hpa_setpoint", "hpa_window_size",
	"http_timeout_ms", "producer_poisson", "tasks_file",
	"apisim_latency_mean_ms", "apisim_latency_jitter_ms",
	"apisim_healthy_window_ms", "apisim_outage_window_ms",
	"apisim_timeout_share", "apisim_reset_share",
	// início do ciclo de falhas do apisim; o cronograma de cada janela, como
	// registrado pelo próprio apisim, fica em failure_windows.csv
	"apisim_started_at",
}

func (r RunResult) row() []string {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	ms := func(d time.Duration) string { return f(float64(d) / float64(time.Millisecond)) }

	row := []string{
		r.RunID,
		r.RoundName,
		strconv.Itoa(r.Repetition),
		r.Status,
		r.Detail,
		r.StartedAt.Format(time.RFC3339Nano),
		r.FinishedAt.Format(time.RFC3339Nano),
		fmt.Sprintf("%.1f", r.FinishedAt.Sub(r.StartedAt).Seconds()),
		strconv.Itoa(r.Messages),
	}

	var strategy, maxRetries, base, max, min, aimdInc, aimdDec, hpaSp, hpaWin, httpTimeout string
	if c := r.Consumer; c != nil {
		strategy, maxRetries = c.BackoffStrategy, strconv.Itoa(c.MaxRetries)
		base, max, min = ms(c.BaseBackoff), ms(c.MaxBackoff), ms(c.MinBackoff)
		aimdInc, aimdDec = ms(c.AIMDIncrement), f(c.AIMDDecreaseFactor)
		hpaSp, hpaWin = f(c.HPASetpoint), strconv.Itoa(c.HPAWindowSize)
		httpTimeout = ms(c.HTTPTimeout)
	}
	var rate, poisson, tasks string
	if p := r.Producer; p != nil {
		rate, poisson, tasks = f(p.ProducerRate), strconv.FormatBool(p.ProducerPoisson), p.TasksFile
	}
	var errRate, pattern, latMean, latJitter, healthy, outage, timeoutShare, resetShare, apisimStart string
	if a := r.Apisim; a != nil {
		errRate, pattern = f(a.ErrorRate), a.Pattern
		latMean, latJitter = f(a.LatencyMeanMs), f(a.LatencyJitterMs)
		healthy, outage = f(a.HealthyWindowMs), f(a.OutageWindowMs)
		timeoutShare, resetShare = f(a.TimeoutShare), f(a.ResetShare)
		apisimStart = a.StartedAt.Format(time.RFC3339Nano)
	}

	return append(row,
		strategy, errRate, pattern, rate, maxRetries,
		base, max, min, aimdInc, aimdDec, hpaSp, hpaWin,
		httpTimeout, poisson, tasks,
		latMean, latJitter, healthy, outage, timeoutShare, resetShare,
		apisimStart,
	)
}

var failureWindowsHeader = []string{"run_id", "window_index", "kind", "start", "end"}

// failureWindow é um intervalo do ciclo de falhas do apisim no padrão outage.
type failureWindow struct {
	Index int
	Kind  string // healthy | outage
	Start time.Time
	End   time.Time
}

// parseWindows lê o arquivo de trocas de janela gravado pelo apisim
// (APISIM_WINDOWS_FILE: linhas "índice,tipo,início") e devolve as janelas que
// começaram antes de until. O fim de cada janela é o início da seguinte; o da
// última é o início mais a duração configurada para aquele tipo de janela.
func parseWindows(data []byte, healthy, outage time.Duration, until time.Time) ([]failureWindow, error) {
	records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
	if err != nil {
		return nil, err
	}
	var out []failureWindow
	for _, rec := range records {
		if len(rec) != 3 {
			return nil, fmt.Errorf("linha de janela inválida: %v", rec)
		}
		idx, err := strconv.Atoi(rec[0])
		if err != nil {
			return nil, fmt.Errorf("índice de janela inválido: %q", rec[0])
		}
		start, err := time.Parse(time.RFC3339Nano, rec[2])
		if err != nil {
			return nil, fmt.Errorf("início de janela inválido: %q", rec[2])
		}
		if !start.Before(until) {
			break
		}
		if n := len(out); n > 0 {
			out[n-1].End = start
		}
		d := healthy
		if rec[1] == "outage" {
			d = outage
		}
		out = append(out, failureWindow{Index: idx, Kind: rec[1], Start: start, End: start.Add(d)})
	}
	return out, nil
}

// failureWindowRows devolve as linhas de failure_windows.csv da execução,
// a partir das trocas de janela registradas pelo próprio apisim: vazio no
// padrão uniforme, ou se o arquivo não foi gravado.
func (r RunResult) failureWindowRows() ([][]string, error) {
	a := r.Apisim
	if a == nil || a.Pattern != "outage" || r.WindowsFile == "" || r.FinishedAt.IsZero() {
		return nil, nil
	}
	data, err := os.ReadFile(r.WindowsFile)
	if err != nil {
		return nil, fmt.Errorf("erro ao ler janelas do apisim %q: %w", r.WindowsFile, err)
	}
	d := func(ms float64) time.Duration { return time.Duration(ms * float64(time.Millisecond)) }
	windows, err := parseWindows(data, d(a.HealthyWindowMs), d(a.OutageWindowMs), r.FinishedAt)
	if err != nil {
		return nil, fmt.Errorf("janelas do apisim %q inválidas: %w", r.WindowsFile, err)
	}
	var rows [][]string
	for _, w := range windows {
		rows = append(rows, []string{
			r.RunID, strconv.Itoa(w.Index), w.Kind,
			w.Start.Format(time.RFC3339Nano), w.End.Format(time.RFC3339Nano),
		})
	}
	return rows, nil
}

// csvWriter grava linhas num CSV com cabeçalho; seguro para uso concorrente
// (várias execuções terminam ao mesmo tempo).
type csvWriter struct {
	mu   sync.Mutex
	file *os.File
	csv  *csv.Writer
}

func newCSVWriter(path string, header []string) (*csvWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar %q: %w", path, err)
	}
	w := &csvWriter{file: f, csv: csv.NewWriter(f)}
	if err := w.csv.Write(header); err != nil {
		f.Close()
		return nil, err
	}
	w.csv.Flush()
	return w, nil
}

func (w *csvWriter) write(rows ...[]string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, row := range rows {
		if err := w.csv.Write(row); err != nil {
			return err
		}
	}
	w.csv.Flush()
	return w.csv.Error()
}

func (w *csvWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.csv.Flush()
	return w.file.Close()
}
