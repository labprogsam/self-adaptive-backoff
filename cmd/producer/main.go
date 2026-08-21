// Command producer lê um arquivo JSON com uma lista de mensagens e publica
// cada uma na fila do RabbitMQ, útil para popular a fila em testes.
package main

import (
	"log"

	"self-adaptive-backoff/internal/config"
	"self-adaptive-backoff/internal/producer"
)

func main() {
	cfg := config.Load()
	if err := producer.Run(cfg); err != nil {
		log.Fatal(err)
	}
}
