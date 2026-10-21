package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	AMQPUrl     string
	QueueName   string
	APIUrl      string
	TasksFile   string
	HTTPTimeout time.Duration

	// Fator A (Design_experimental.md, Seção 5) — estratégia de backoff:
	// none | fixed | exponential | exponential_jitter | aimd | hpa.
	BackoffStrategy string
	BaseBackoff     time.Duration
	MaxBackoff      time.Duration
	// MinBackoff é o piso do estado adaptativo do AIMD e do algoritmo do
	// HPA (Seção 4.1) — não usado pelas estratégias estáticas.
	MinBackoff time.Duration

	// Fator E — número máximo de tentativas extras antes do descarte.
	MaxRetries int

	// Parâmetros específicos do AIMD (Fator A, nível 5).
	AIMDIncrement      time.Duration
	AIMDDecreaseFactor float64

	// Parâmetros específicos do algoritmo do HPA (Fator A, nível 6) — sem
	// ganho adicional, fórmula original do Kubernetes (ver Seção 4.1).
	HPASetpoint   float64
	HPAWindowSize int

	// Identificação da execução e saída estruturada (Seção 8 — schema de
	// saída por tentativa, agrupado por run_id).
	RunID       string
	MetricsFile string

	// Carga de trabalho do producer (Seções 4.2 e 7): publica as mensagens
	// de TasksFile no ritmo de ProducerRate msgs/s.
	ProducerRate float64
	// ProducerPoisson usa intervalos entre publicações exponencialmente
	// distribuídos (chegada Poisson, Seção 7) em vez de intervalo fixo.
	ProducerPoisson bool
}

// Load lê a configuração das variáveis de ambiente do processo. Os defaults de
// tempo estão na escala 1/5 validada no piloto 02
// (pilots/02_escala_e_capacidade) e os do AIMD/HPA são os calibrados no piloto
// 03 (pilots/03_calibracao_aimd_hpa).
func Load() Config {
	return LoadFrom(os.Getenv)
}

// LoadFrom é Load com uma fonte de variáveis explícita — usado pelo
// orquestrador para calcular a configuração efetiva de cada processo filho a
// partir do mesmo ambiente que ele recebe (registro por execução, Seção 8).
func LoadFrom(getenv func(string) string) Config {
	e := env(getenv)
	return Config{
		AMQPUrl:     e.get("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:   e.get("QUEUE_NAME", "tasks"),
		APIUrl:      e.get("API_URL", "http://localhost:9091/process"),
		TasksFile:   e.get("TASKS_FILE", "tasks.json"),
		HTTPTimeout: e.getDuration("HTTP_TIMEOUT", 6*time.Second),

		BackoffStrategy: e.get("BACKOFF_STRATEGY", "exponential_jitter"),
		BaseBackoff:     e.getDuration("BASE_BACKOFF", 100*time.Millisecond),
		MaxBackoff:      e.getDuration("MAX_BACKOFF", 1600*time.Millisecond),
		MinBackoff:      e.getDuration("MIN_BACKOFF", 10*time.Millisecond),

		MaxRetries: e.getInt("MAX_RETRIES", 3),

		AIMDIncrement:      e.getDuration("AIMD_INCREMENT", 100*time.Millisecond),
		AIMDDecreaseFactor: e.getFloat("AIMD_DECREASE_FACTOR", 0.5),

		HPASetpoint:   e.getFloat("HPA_SETPOINT", 0.5),
		HPAWindowSize: e.getInt("HPA_WINDOW_SIZE", 50),

		RunID:       e.get("RUN_ID", defaultRunID()),
		MetricsFile: e.get("METRICS_FILE", "metrics/attempts.csv"),

		ProducerRate:    e.getFloat("PRODUCER_RATE", 2.5),
		ProducerPoisson: e.getBool("PRODUCER_POISSON", true),
	}
}

func defaultRunID() string {
	return time.Now().Format("20060102T150405")
}

// env lê variáveis com fallback para o default quando ausentes ou inválidas.
type env func(string) string

func (e env) get(key, fallback string) string {
	if v := e(key); v != "" {
		return v
	}
	return fallback
}

func (e env) getDuration(key string, fallback time.Duration) time.Duration {
	if v := e(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func (e env) getInt(key string, fallback int) int {
	if v := e(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func (e env) getFloat(key string, fallback float64) float64 {
	if v := e(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func (e env) getBool(key string, fallback bool) bool {
	if v := e(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
