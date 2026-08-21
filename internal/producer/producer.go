package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"

	"self-adaptive-backoff/internal/config"
)

func Run(cfg config.Config) error {
	messages, err := loadMessages(cfg.TasksFile)
	if err != nil {
		return fmt.Errorf("erro ao ler arquivo %q: %w", cfg.TasksFile, err)
	}

	conn, err := amqp.Dial(cfg.AMQPUrl)
	if err != nil {
		return fmt.Errorf("erro ao conectar no RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("erro ao abrir canal: %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(cfg.QueueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("erro ao declarar fila: %w", err)
	}

	for i, msg := range messages {
		err := ch.Publish("", q.Name, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		})
		if err != nil {
			return fmt.Errorf("erro ao publicar mensagem %d/%d: %w", i+1, len(messages), err)
		}
		fmt.Printf("mensagem %d/%d publicada: %s\n", i+1, len(messages), string(msg))
	}

	log.Printf("%d mensagens publicadas na fila %q", len(messages), q.Name)
	return nil
}

func loadMessages(path string) ([]json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("conteúdo não é um array JSON válido: %w", err)
	}

	return items, nil
}
