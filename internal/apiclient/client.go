package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Erros sentinela usados para classificar o resultado de cada tentativa
// (Design_experimental.md, Seção 8 — schema de saída por tentativa: sucesso,
// timeout, erro de comunicação, erro de dado). Envolvidos com %w para que
// errors.Is funcione mesmo com a mensagem original anexada.
var (
	ErrInvalidData   = errors.New("erro de dado")
	ErrTimeout       = errors.New("timeout")
	ErrCommunication = errors.New("erro de comunicação")
	ErrAPIStatus     = errors.New("api retornou erro")
)

type Client struct {
	httpClient *http.Client
	url        string
	timeout    time.Duration
}

func New(url string, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: timeout},
		url:        url,
		timeout:    timeout,
	}
}

func (c *Client) Process(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("%w: JSON inválido: %v", ErrInvalidData, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("erro ao montar requisição: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrCommunication, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%w: erro ao ler resposta: %v", ErrCommunication, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: status %d: %s", ErrAPIStatus, resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// Classify mapeia o erro de uma tentativa para uma das categorias do schema
// de saída por tentativa (Design_experimental.md, Seção 8). Um erro nil
// classifica como sucesso. Uma resposta HTTP de erro (ErrAPIStatus) é
// contabilizada junto com erro de comunicação: do ponto de vista do
// mecanismo de backoff, ambos significam "a API de terceiro não processou a
// requisição", a distinção entre nível de transporte e nível de aplicação
// não é relevante para a decisão de retry.
func Classify(err error) string {
	switch {
	case err == nil:
		return "sucesso"
	case errors.Is(err, ErrInvalidData):
		return "erro_dado"
	case errors.Is(err, ErrTimeout):
		return "timeout"
	case errors.Is(err, ErrCommunication), errors.Is(err, ErrAPIStatus):
		return "erro_comunicacao"
	default:
		return "erro_desconhecido"
	}
}
