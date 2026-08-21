package main

import (
	"self-adaptive-backoff/internal/config"
	"self-adaptive-backoff/internal/consumer"
)

func main() {
	cfg := config.Load()
	consumer.Run(cfg)
}
