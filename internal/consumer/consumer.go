package consumer

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"self-adaptive-backoff/internal/apiclient"
	"self-adaptive-backoff/internal/backoff"
	"self-adaptive-backoff/internal/config"
)

func Run(cfg config.Config) {
	client := apiclient.New(cfg.APIUrl, cfg.HTTPTimeout)

	log.Printf("iniciando consumidor | fila=%s api=%s", cfg.QueueName, cfg.APIUrl)

	for {
		if err := consumeOnce(cfg, client); err != nil {
			log.Printf("consumidor encerrado com erro: %v", err)
		}
		log.Printf("tentando reconectar ao RabbitMQ em %s...", cfg.BaseBackoff)
		time.Sleep(cfg.BaseBackoff)
	}
}

func consumeOnce(cfg config.Config, client *apiclient.Client) error {
	conn, err := amqp.Dial(cfg.AMQPUrl)
	if err != nil {
		return fmt.Errorf("falha ao conectar no RabbitMQ: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("falha ao abrir canal: %w", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		cfg.QueueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("falha ao declarar fila: %w", err)
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("falha ao configurar QoS: %w", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		false, // auto-ack (vamos dar ack manual após processar)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("falha ao registrar consumidor: %w", err)
	}

	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

	log.Printf("aguardando mensagens na fila %q...", q.Name)

	for {
		select {
		case amqpErr := <-connClosed:
			return fmt.Errorf("conexão com RabbitMQ perdida: %v", amqpErr)
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("canal de mensagens fechado")
			}
			handleDelivery(cfg, client, d)
		}
	}
}

func handleDelivery(cfg config.Config, client *apiclient.Client, d amqp.Delivery) {
	log.Printf("mensagem recebida: %s", string(d.Body))

	opts := backoff.Options{
		MaxRetries: cfg.MaxRetries,
		BaseDelay:  cfg.BaseBackoff,
		MaxDelay:   cfg.MaxBackoff,
	}

	result, err := backoff.Retry(opts, func() ([]byte, error) {
		return client.Process(d.Body)
	})

	if err != nil {
		log.Printf("falha ao processar mensagem após retries: %v", err)
		// Descarta a mensagem (não requeue) para evitar loop infinito de poison message.
		_ = d.Nack(false, false)
		return
	}

	fmt.Printf("resultado da API: %s\n", string(result))
	_ = d.Ack(false)
}
