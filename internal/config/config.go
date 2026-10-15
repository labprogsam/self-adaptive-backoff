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

	// Carga de trabalho do producer (Seção 7.1 / 4.2): publica as mensagens
	// de TasksFile no ritmo de ProducerRate msgs/s.
	ProducerRate float64
	// ProducerPoisson usa intervalos entre publicações exponencialmente
	// distribuídos (chegada Poisson, Seção 7.1) em vez de intervalo fixo.
	ProducerPoisson bool
}

func Load() Config {
	return Config{
		AMQPUrl:     getEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:   getEnv("QUEUE_NAME", "tasks"),
		APIUrl:      getEnv("API_URL", "http://localhost:9091/process"),
		TasksFile:   getEnv("TASKS_FILE", "tasks.json"),
		HTTPTimeout: getEnvDuration("HTTP_TIMEOUT", 30*time.Second),

		BackoffStrategy: getEnv("BACKOFF_STRATEGY", "exponential_jitter"),
		BaseBackoff:     getEnvDuration("BASE_BACKOFF", 500*time.Millisecond),
		MaxBackoff:      getEnvDuration("MAX_BACKOFF", 30*time.Second),
		MinBackoff:      getEnvDuration("MIN_BACKOFF", 50*time.Millisecond),

		MaxRetries: getEnvInt("MAX_RETRIES", 3),

		AIMDIncrement:      getEnvDuration("AIMD_INCREMENT", 100*time.Millisecond),
		AIMDDecreaseFactor: getEnvFloat("AIMD_DECREASE_FACTOR", 0.5),

		HPASetpoint:   getEnvFloat("HPA_SETPOINT", 0.05),
		HPAWindowSize: getEnvInt("HPA_WINDOW_SIZE", 20),

		RunID:       getEnv("RUN_ID", defaultRunID()),
		MetricsFile: getEnv("METRICS_FILE", "metrics/attempts.csv"),

		ProducerRate:    getEnvFloat("PRODUCER_RATE", 20),
		ProducerPoisson: getEnvBool("PRODUCER_POISSON", true),
	}
}

func defaultRunID() string {
	return time.Now().Format("20060102T150405")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
