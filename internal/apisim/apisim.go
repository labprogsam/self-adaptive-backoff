// Package apisim implementa o simulador de API de terceiro descrito em
// Design_experimental.md, Seção 7.2: um serviço HTTP que decide, por
// configuração, se cada requisição recebida responde com sucesso ou falha,
// segundo a taxa de erro (Fator B) e o padrão temporal de falha (Fator C),
// com latência de resposta configurável (Seção 4.2).
//
// É um serviço separado do restante do projeto — não depende de
// internal/apiclient nem de internal/config, para poder ser executado de
// forma independente do consumidor/producer durante os experimentos.
package apisim

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Config agrupa os parâmetros do simulador.
type Config struct {
	Addr string

	// ErrorRate é a taxa de erro "de base" (Fator B): usada diretamente no
	// padrão uniform, e como taxa da janela saudável no padrão outage.
	ErrorRate float64
	// Pattern é o Fator C: uniform | outage.
	Pattern string

	// Padrão outage: ciclos de HealthyWindow (taxa ErrorRate) seguidos de
	// OutageWindow com 100% de falha (Seção 7.2).
	HealthyWindow time.Duration
	OutageWindow  time.Duration

	// Distribuição de latência de resposta em caso de sucesso (Seção 4.2).
	LatencyMean   time.Duration
	LatencyJitter time.Duration

	// Dentre as requisições que falham, frações que se manifestam como
	// timeout (resposta muito lenta) ou reset abrupto de conexão (erro de
	// comunicação), em vez de uma resposta HTTP de erro comum. O restante
	// (1 - TimeoutShare - ResetShare) responde com status 5xx. Todos opcionais
	// (default 0 = só 5xx), para manter o comportamento padrão simples.
	TimeoutShare float64
	TimeoutDelay time.Duration
	ResetShare   float64
}

func LoadConfig() Config {
	return Config{
		Addr: getEnv("APISIM_ADDR", ":9091"),

		ErrorRate: getEnvFloat("APISIM_ERROR_RATE", 0.1),
		Pattern:   getEnv("APISIM_PATTERN", "uniform"),

		HealthyWindow: getEnvDuration("APISIM_HEALTHY_WINDOW", 10*time.Second),
		OutageWindow:  getEnvDuration("APISIM_OUTAGE_WINDOW", 10*time.Second),

		LatencyMean:   getEnvDuration("APISIM_LATENCY_MEAN", 50*time.Millisecond),
		LatencyJitter: getEnvDuration("APISIM_LATENCY_JITTER", 20*time.Millisecond),

		TimeoutShare: getEnvFloat("APISIM_TIMEOUT_SHARE", 0),
		TimeoutDelay: getEnvDuration("APISIM_TIMEOUT_DELAY", 5*time.Second),
		ResetShare:   getEnvFloat("APISIM_RESET_SHARE", 0),
	}
}

// Server implementa http.Handler segundo Config.
type Server struct {
	cfg   Config
	start time.Time
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg, start: time.Now()}
}

// currentErrorRate calcula a taxa de erro vigente agora, conforme o Fator C.
func (s *Server) currentErrorRate() float64 {
	switch s.cfg.Pattern {
	case "outage":
		cycle := s.cfg.HealthyWindow + s.cfg.OutageWindow
		if cycle <= 0 {
			return s.cfg.ErrorRate
		}
		elapsed := time.Since(s.start) % cycle // posição atual dentro do ciclo
		if elapsed < s.cfg.HealthyWindow {
			return s.cfg.ErrorRate // janela saudável: usa ErrorRate
		}
		return 1.0 // janela de indisponibilidade: 100%, ErrorRate ignorado
	default: // uniform
		return s.cfg.ErrorRate
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// O conteúdo do corpo não importa para a simulação; só precisa ser
	// consumido para a conexão poder ser reaproveitada.
	_, _ = io.Copy(io.Discard, r.Body)
	_ = r.Body.Close()

	latency := s.cfg.LatencyMean
	if s.cfg.LatencyJitter > 0 {
		latency += time.Duration(rand.Int63n(int64(s.cfg.LatencyJitter)))
	}

	if rand.Float64() >= s.currentErrorRate() {
		time.Sleep(latency)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
		return
	}

	roll := rand.Float64()
	switch {
	case roll < s.cfg.ResetShare:
		hj, ok := w.(http.Hijacker)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn.Close()
	case roll < s.cfg.ResetShare+s.cfg.TimeoutShare:
		time.Sleep(s.cfg.TimeoutDelay)
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.Write([]byte(`{"error":"simulated timeout"}`))
	default:
		time.Sleep(latency)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"simulated failure"}`))
	}
}

func Run(cfg Config) error {
	if cfg.Pattern != "uniform" && cfg.Pattern != "outage" {
		return fmt.Errorf("APISIM_PATTERN inválido: %q (esperado: uniform | outage)", cfg.Pattern)
	}
	s := New(cfg)
	log.Printf("apisim: escutando em %s | pattern=%s error_rate=%.2f latency=%s±%s",
		cfg.Addr, cfg.Pattern, cfg.ErrorRate, cfg.LatencyMean, cfg.LatencyJitter)
	if cfg.Pattern == "outage" {
		log.Printf("apisim: janela saudável=%s janela de indisponibilidade=%s", cfg.HealthyWindow, cfg.OutageWindow)
	}
	return http.ListenAndServe(cfg.Addr, s)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
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

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
