// Command apisim é o simulador de API de terceiro usado nos experimentos
// (Design_experimental.md, Seção 7.2): injeta falhas e latência configuráveis
// no lugar de uma API real, para que os Fatores B e C sejam controlados de
// forma determinística entre execuções.
package main

import (
	"log"

	"self-adaptive-backoff/internal/apisim"
)

func main() {
	cfg := apisim.LoadConfig()
	if err := apisim.Run(cfg); err != nil {
		log.Fatal(err)
	}
}
