package config

import (
	"os"
	"time"
)

type Config struct {
	AMQPUrl     string
	QueueName   string
	APIUrl      string
	TasksFile   string
	HTTPTimeout time.Duration
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func Load() Config {
	return Config{
		AMQPUrl:     getEnv("AMQP_URL", "amqp://guest:guest@localhost:5672/"),
		QueueName:   getEnv("QUEUE_NAME", "tasks"),
		APIUrl:      getEnv("API_URL", "http://localhost:9091/process"),
		TasksFile:   getEnv("TASKS_FILE", "tasks.json"),
		HTTPTimeout: 30 * time.Second,
		MaxRetries:  3,
		BaseBackoff: 500 * time.Millisecond,
		MaxBackoff:  30 * time.Second,
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
