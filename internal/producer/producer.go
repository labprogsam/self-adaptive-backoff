package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"self-adaptive-backoff/internal/config"
)

func Run(cfg config.Config) error {
	messages, err := loadMessages(cfg.TasksFile)
	if err != nil {
		return fmt.Errorf("erro ao ler arquivo %q: %w", cfg.TasksFile, err)
	}
	log.Printf("producer: %d mensagens lidas de %s", len(messages), cfg.TasksFile)

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
		if i > 0 && cfg.ProducerRate > 0 {
			time.Sleep(nextInterval(cfg.ProducerRate, cfg.ProducerPoisson))
		}

		err := ch.Publish("", q.Name, false, false, amqp.Publishing{
			ContentType: "application/json",
			MessageId:   fmt.Sprintf("%s-%d", cfg.RunID, i),
			Body:        msg,
		})
		if err != nil {
			return fmt.Errorf("erro ao publicar mensagem %d/%d: %w", i+1, len(messages), err)
		}
		if len(msg) <= 200 {
			fmt.Printf("mensagem %d/%d publicada: %s\n", i+1, len(messages), string(msg))
		} else {
			fmt.Printf("mensagem %d/%d publicada (%d bytes)\n", i+1, len(messages), len(msg))
		}
	}

	log.Printf("%d mensagens publicadas na fila %q (run_id=%s)", len(messages), q.Name, cfg.RunID)
	return nil
}

// nextInterval calcula o intervalo até a próxima publicação para a taxa
// (mensagens/segundo) e o padrão de chegada configurados (Seção 7.1). Com
// poisson=true, o intervalo é exponencialmente distribuído com média
// 1/rate, simulando chegadas independentes (Poisson); com poisson=false, o
// intervalo é fixo em 1/rate.
func nextInterval(rate float64, poisson bool) time.Duration {
	mean := time.Duration(float64(time.Second) / rate)
	if !poisson {
		return mean
	}
	u := rand.Float64()
	if u <= 0 {
		u = 1e-9
	}
	return time.Duration(-math.Log(u) * float64(mean))
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
