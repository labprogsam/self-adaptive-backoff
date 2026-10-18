// Command orchestrator roda um plano de experimento (internal/orchestrator):
// várias rodadas (combinações de fatores × repetições), cada uma uma
// execução isolada e sequencial de apisim+consumer+producer, com paralelismo
// limitado entre rodadas. Deve ser rodado a partir da raiz do repositório
// (precisa compilar cmd/consumer, cmd/producer e cmd/apisim).
package main

import (
	"flag"
	"log"
	"time"

	"self-adaptive-backoff/internal/orchestrator"
)

func main() {
	plan := flag.String("plan", "", "caminho do arquivo de plano (JSON)")
	parallel := flag.Int("parallel", 4, "número de rodadas executadas simultaneamente")
	binDir := flag.String("bin-dir", "bin", "diretório onde os binários (consumer/producer/apisim) são compilados")
	metricsDir := flag.String("metrics-dir", "metrics", "diretório de saída dos CSVs de métricas e do resumo")
	logDir := flag.String("log-dir", "", "diretório de saída dos logs de cada processo (vazio = sem logs)")
	drainTimeout := flag.Duration("drain-timeout", 20*time.Minute, "tempo máximo de espera por rodada até todas as mensagens serem processadas")
	repoRoot := flag.String("repo-root", ".", "diretório raiz do repositório (para compilar os binários)")
	shuffle := flag.Bool("shuffle", true, "embaralha a ordem das execuções (Design_experimental.md, Seção 8.2)")
	seed := flag.Int64("seed", 0, "semente do embaralhamento (0 = gera uma a partir do relógio; a usada fica em execution_order.csv)")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	if *plan == "" {
		log.Fatal("uso: orchestrator -plan caminho/para/plano.json [-parallel N] [-drain-timeout 20m]")
	}

	cfg := orchestrator.Config{
		PlanPath:     *plan,
		Parallelism:  *parallel,
		BinDir:       *binDir,
		MetricsDir:   *metricsDir,
		LogDir:       *logDir,
		DrainTimeout: *drainTimeout,
		RepoRoot:     *repoRoot,
		Shuffle:      *shuffle,
		Seed:         *seed,
	}

	if err := orchestrator.Run(cfg); err != nil {
		log.Fatal(err)
	}
}
