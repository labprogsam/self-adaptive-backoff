package orchestrator

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// deleteQueue apaga a fila de uma execução no RabbitMQ, com as mensagens que
// ainda estiverem nela. É chamada antes de a execução começar — para que ela
// nunca consuma mensagens deixadas por uma execução anterior com o mesmo
// run_id (ex.: uma que terminou por timeout) — e depois que ela termina, para
// não acumular filas no broker. Apagar uma fila que não existe não é erro.
func deleteQueue(amqpURL, name string) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("erro ao conectar no RabbitMQ para apagar a fila %q: %w", name, err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("erro ao abrir canal para apagar a fila %q: %w", name, err)
	}
	defer ch.Close()

	if _, err := ch.QueueDelete(name, false, false, false); err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
			return nil
		}
		return fmt.Errorf("erro ao apagar a fila %q: %w", name, err)
	}
	return nil
}
