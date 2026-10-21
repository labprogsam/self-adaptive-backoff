// Package apisim implementa o simulador de API de terceiro descrito em
// Design_experimental.md, Seção 7: um serviço HTTP que decide, por
// configuração, se cada requisição recebida responde com sucesso ou falha,
// segundo a taxa de erro (Fator B) e o padrão temporal de falha (Fator C),
// com latência de resposta configurável (Seção 4.2).
//
// É um serviço separado do restante do projeto — não depende de
// internal/apiclient nem de internal/config, para poder ser executado de
// forma independente do consumidor/producer durante os experimentos.
package apisim

import (
	"encoding/json"
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
	// OutageWindow com 100% de falha (Seção 7).
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

	// InfoFile, se não vazio, recebe um JSON (Info) com o instante em que o
	// ciclo de falhas começou e a configuração efetiva — é o que permite
	// reconstruir o cronograma exato das janelas de indisponibilidade de cada
	// execução (registro por execução, Seção 8). Opcional.
	InfoFile string
	// WindowsFile, se não vazio (e o padrão for outage), recebe uma linha CSV
	// "índice,tipo,início" a cada troca de janela, com o relógio de parede do
	// momento da troca. Registrar cada troca quando ela acontece (em vez de
	// recalcular o cronograma depois a partir de StartedAt) mantém as janelas
	// alinhadas com os timestamps das tentativas mesmo se o relógio de parede
	// for ajustado durante a execução — as janelas em si são medidas com o
	// relógio monotônico. Opcional.
	WindowsFile string
	// ID identifica esta instância: é devolvido por /__health, para quem a
	// iniciou confirmar que está falando com ela (e não com outro processo na
	// mesma porta). Opcional.
	ID string
}

// Info é o conteúdo gravado em Config.InfoFile. Durações em milissegundos.
type Info struct {
	StartedAt       time.Time `json:"started_at"`
	Pattern         string    `json:"pattern"`
	ErrorRate       float64   `json:"error_rate"`
	HealthyWindowMs float64   `json:"healthy_window_ms"`
	OutageWindowMs  float64   `json:"outage_window_ms"`
	LatencyMeanMs   float64   `json:"latency_mean_ms"`
	LatencyJitterMs float64   `json:"latency_jitter_ms"`
	TimeoutShare    float64   `json:"timeout_share"`
	TimeoutDelayMs  float64   `json:"timeout_delay_ms"`
	ResetShare      float64   `json:"reset_share"`
}

// LoadConfig lê a configuração das variáveis de ambiente. Os defaults de tempo
// estão na escala 1/5 validada no piloto 02 (pilots/02_escala_e_capacidade).
func LoadConfig() Config {
	return Config{
		Addr: getEnv("APISIM_ADDR", ":9091"),

		ErrorRate: getEnvFloat("APISIM_ERROR_RATE", 0.1),
		Pattern:   getEnv("APISIM_PATTERN", "uniform"),

		HealthyWindow: getEnvDuration("APISIM_HEALTHY_WINDOW", 2*time.Second),
		OutageWindow:  getEnvDuration("APISIM_OUTAGE_WINDOW", 2*time.Second),

		LatencyMean:   getEnvDuration("APISIM_LATENCY_MEAN", 10*time.Millisecond),
		LatencyJitter: getEnvDuration("APISIM_LATENCY_JITTER", 4*time.Millisecond),

		TimeoutShare: getEnvFloat("APISIM_TIMEOUT_SHARE", 0),
		TimeoutDelay: getEnvDuration("APISIM_TIMEOUT_DELAY", 1*time.Second),
		ResetShare:   getEnvFloat("APISIM_RESET_SHARE", 0),

		InfoFile: getEnv("APISIM_INFO_FILE", ""),
		ID:       getEnv("APISIM_ID", ""),

		WindowsFile: getEnv("APISIM_WINDOWS_FILE", ""),
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

// Info descreve o servidor: instante de início do ciclo de falhas e
// configuração efetiva.
func (s *Server) Info() Info {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return Info{
		StartedAt:       s.start,
		Pattern:         s.cfg.Pattern,
		ErrorRate:       s.cfg.ErrorRate,
		HealthyWindowMs: ms(s.cfg.HealthyWindow),
		OutageWindowMs:  ms(s.cfg.OutageWindow),
		LatencyMeanMs:   ms(s.cfg.LatencyMean),
		LatencyJitterMs: ms(s.cfg.LatencyJitter),
		TimeoutShare:    s.cfg.TimeoutShare,
		TimeoutDelayMs:  ms(s.cfg.TimeoutDelay),
		ResetShare:      s.cfg.ResetShare,
	}
}

// recordWindows grava cada troca de janela do padrão outage no momento em que
// ela acontece. Os instantes das trocas são calculados a partir de s.start
// com o relógio monotônico (o mesmo que currentErrorRate usa); o horário
// gravado é o relógio de parede no momento da troca.
func (s *Server) recordWindows(f *os.File) {
	durations := [2]time.Duration{s.cfg.HealthyWindow, s.cfg.OutageWindow}
	kinds := [2]string{"healthy", "outage"}
	if durations[0]+durations[1] <= 0 {
		return
	}
	var offset time.Duration
	for i := 0; ; i++ {
		d := durations[i%2]
		if d <= 0 {
			continue
		}
		time.Sleep(time.Until(s.start.Add(offset)))
		now := time.Now().Round(0) // só o relógio de parede
		if _, err := fmt.Fprintf(f, "%d,%s,%s\n", i, kinds[i%2], now.Format(time.RFC3339Nano)); err != nil {
			log.Printf("apisim: erro ao gravar janela: %v", err)
			return
		}
		offset += d
	}
}

// writeInfo grava o arquivo de forma atômica (arquivo temporário + rename),
// para que quem o lê nunca veja um JSON pela metade.
func writeInfo(path string, info Info) error {
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("erro ao gravar %q: %w", path, err)
	}
	return os.Rename(tmp, path)
}

// HealthPath responde na hora com 200 e o ID da instância, fora da simulação
// de falhas e latência — não é chamado pelo consumer, só por quem precisa
// saber se o apisim já está pronto.
const HealthPath = "/__health"

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == HealthPath {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(s.cfg.ID))
		return
	}

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
	if cfg.InfoFile != "" {
		if err := writeInfo(cfg.InfoFile, s.Info()); err != nil {
			return err
		}
	}
	if cfg.WindowsFile != "" && cfg.Pattern == "outage" {
		f, err := os.Create(cfg.WindowsFile)
		if err != nil {
			return fmt.Errorf("erro ao criar %q: %w", cfg.WindowsFile, err)
		}
		defer f.Close()
		go s.recordWindows(f)
	}
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
