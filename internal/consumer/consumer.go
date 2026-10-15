package consumer

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"self-adaptive-backoff/internal/apiclient"
	"self-adaptive-backoff/internal/backoff"
	"self-adaptive-backoff/internal/config"
	"self-adaptive-backoff/internal/metrics"
)

func Run(cfg config.Config) {
	client := apiclient.New(cfg.APIUrl, cfg.HTTPTimeout)

	strategy, err := backoff.New(cfg.BackoffStrategy, backoff.Params{
		BaseDelay:          cfg.BaseBackoff,
		MaxDelay:           cfg.MaxBackoff,
		MinDelay:           cfg.MinBackoff,
		AIMDIncrement:      cfg.AIMDIncrement,
		AIMDDecreaseFactor: cfg.AIMDDecreaseFactor,
		HPASetpoint:        cfg.HPASetpoint,
		HPAWindowSize:      cfg.HPAWindowSize,
	})
	if err != nil {
		log.Fatalf("configuração de backoff inválida: %v", err)
	}

	writer, err := metrics.NewWriter(cfg.MetricsFile)
	if err != nil {
		log.Fatalf("erro ao abrir arquivo de métricas: %v", err)
	}
	defer writer.Close()

	log.Printf("iniciando consumidor | fila=%s api=%s estrategia=%s run_id=%s",
		cfg.QueueName, cfg.APIUrl, strategy.Name(), cfg.RunID)

	// strategy e writer são criados uma única vez e compartilhados por toda
	// a execução do consumidor (inclusive entre reconexões do RabbitMQ): o
	// estado do AIMD/HPA é sobre o histórico observado de chamadas à API,
	// não sobre a conexão com o broker.
	for {
		if err := consumeOnce(cfg, client, strategy, writer); err != nil {
			log.Printf("consumidor encerrado com erro: %v", err)
		}
		log.Printf("tentando reconectar ao RabbitMQ em %s...", cfg.BaseBackoff)
		time.Sleep(cfg.BaseBackoff)
	}
}

func consumeOnce(cfg config.Config, client *apiclient.Client, strategy backoff.Strategy, writer *metrics.Writer) error {
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
			handleDelivery(cfg, client, strategy, writer, d)
		}
	}
}

func handleDelivery(cfg config.Config, client *apiclient.Client, strategy backoff.Strategy, writer *metrics.Writer, d amqp.Delivery) {
	messageID := d.MessageId
	if messageID == "" {
		messageID = "sem-id"
	}

	log.Printf("mensagem recebida: id=%s tamanho=%d bytes", messageID, len(d.Body))

	outcome := backoff.Backoff(strategy, cfg.MaxRetries, func() ([]byte, error) {
		return client.Process(d.Body)
	})

	finalResult := classifyFinalResult(outcome)
	for i, a := range outcome.Attempts {
		record := metrics.AttemptRecord{
			RunID:         cfg.RunID,
			MessageID:     messageID,
			AttemptNumber: a.Number,
			Timestamp:     a.StartedAt,
			Duration:      a.Duration,
			WaitAfter:     a.WaitAfter,
			AttemptResult: apiclient.Classify(a.Err),
		}
		if i == len(outcome.Attempts)-1 {
			record.FinalResult = finalResult
		}
		if err := writer.WriteAttempt(record); err != nil {
			log.Printf("erro ao gravar métrica da tentativa %d da mensagem %s: %v", a.Number, messageID, err)
		}
	}

	if outcome.Err != nil {
		log.Printf("falha ao processar mensagem %s após %d tentativa(s): %v", messageID, len(outcome.Attempts), outcome.Err)
		// Descarta a mensagem (não requeue) para evitar loop infinito de poison message.
		_ = d.Nack(false, false)
		return
	}

	fmt.Printf("mensagem %s processada com sucesso (%d tentativa(s), %d bytes de resposta)\n", messageID, len(outcome.Attempts), len(outcome.Result))
	_ = d.Ack(false)
}

// classifyFinalResult mapeia o Outcome para o resultado terminal da mensagem
// (Design_experimental.md, Seção 2): R1 sucesso imediato, R2 sucesso após
// retry, R3 esgotamento.
func classifyFinalResult(outcome backoff.Outcome[[]byte]) string {
	if outcome.Err != nil {
		return "R3"
	}
	if len(outcome.Attempts) == 1 {
		return "R1"
	}
	return "R2"
}
