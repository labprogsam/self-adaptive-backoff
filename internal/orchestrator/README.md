O orchestrator é o "maestro" dos experimentos. Ele lê um plano, monta cada execução, sobe os programas necessários, espera tudo terminar e anota o resultado.

O que cada parte faz

cmd/orchestrator/main.go: a porta de entrada.
Recebe as opções da linha de comando: qual plano usar, quantas execuções rodar ao mesmo tempo (-parallel), onde salvar métricas e logs e quanto tempo esperar no máximo por execução (-drain-timeout). Depois passa tudo para o orquestrador.

plan.go: o plano do experimento.
- Plan: o arquivo JSON com as combinações de fatores (as "rodadas"), quantas repetições cada uma terá e as configurações comuns a todas.
- Round: uma combinação de fatores, por exemplo "estratégia X com falha tipo Y". Cada rodada diz como configurar o consumer, o apisim e, se precisar, o producer.
- LoadPlan: lê o arquivo e confere se está correto: tem repetições, tem rodadas, nenhuma rodada sem nome ou com nome repetido.
- Expand: transforma "rodadas × repetições" numa lista de execuções. Cada uma ganha um nome único, como rodadaX_rep03. Esse nome (o RunID) é a chave de todo o isolamento.

orchestrator.go: o coração.
- Run: cria as pastas, compila os três programas uma única vez e dispara as execuções, mantendo no máximo N rodando ao mesmo tempo. Ele funciona como um estacionamento com N vagas: uma execução nova só entra quando outra sai.
- buildBinaries: compila consumer, producer e apisim. Todas as execuções usam exatamente o mesmo código.
- runExecution: roda uma execução do começo ao fim, sempre na mesma sequência:
  a. reserva uma porta livre e sobe o apisim (a API falsa que simula falhas) nela;
  b. sobe o consumer apontando para essa API e para uma fila própria;
  c. espera meio segundo para o consumer se conectar;
  d. roda o producer, que publica as mensagens e termina;
  e. espera todas as mensagens serem processadas;
  f. desliga o apisim e o consumer.
- expectedMessageCount: conta quantas tarefas existem no arquivo de tarefas, para saber quando a execução acabou.

process.go: cuida dos programas.
- freePort / waitApisimReady: acham uma porta livre e esperam o apisim responder em `/__health` com o ID da execução (não basta a porta aceitar conexão TCP: no loopback, uma auto-conexão TCP pode fazer a porta parecer aberta antes do apisim subir).
- buildEnv: monta as configurações de cada programa. A ordem de prioridade é: configurações gerais, depois as da rodada, depois as exclusivas da execução, e a última vence.
- startProcess: inicia um programa e grava a saída dele num log próprio, para não misturar com a de outras execuções.
- stop: pede educadamente para o programa encerrar; se ele não sair em 5 segundos, força o encerramento.

drain.go: detecta o fim de uma execução.
Lê periodicamente o CSV de métricas e conta quantas mensagens já têm um resultado final (sucesso ou desistência). Quando esse número chega ao total esperado, a execução terminou. Se passar do tempo limite, ela é marcada como timeout.

summary.go: o caderno de anotações.
Grava uma linha por execução em runs_summary.csv, com status (ok, timeout ou error), início, fim e duração. Uma trava garante que duas execuções nunca escrevam ao mesmo tempo, o que embaralharia o arquivo.

Como o paralelismo não contamina os resultados

A ideia central é que o paralelismo acontece entre execuções, nunca dentro de uma. Cada execução continua processando uma mensagem por vez, como o design experimental exige. As estratégias adaptativas aprendem com as mensagens anteriores, e intercalar mensagens de outra execução estragaria esse aprendizado.

Cada execução tem o seu próprio "mundo", separado das outras:

┌───────────────────┬─────────────────────────────────────────────────────────────────────────────────┐
│      Recurso      │                               Como fica separado                                │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ API simulada      │ Um processo próprio, numa porta própria. As falhas que uma execução sofre não   │
│ (apisim)          │ afetam a outra.                                                                 │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ Fila no RabbitMQ  │ tasks_<RunID>: o consumer de uma execução nunca pega mensagem de outra.         │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ Consumer          │ Um processo próprio, então o estado interno da estratégia (o que ela            │
│                   │ "aprendeu") não é compartilhado.                                                │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ Arquivo de        │ <rodada>/<RunID>.csv: cada execução escreve no seu.                             │
│ métricas          │                                                                                 │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ Logs              │ Um arquivo por programa por execução.                                           │
├───────────────────┼─────────────────────────────────────────────────────────────────────────────────┤
│ Resumo            │ Arquivo único, mas protegido pela trava.                                        │
└───────────────────┴─────────────────────────────────────────────────────────────────────────────────┘

Na prática, rodar 4 execuções em paralelo é como ter 4 laboratórios completos lado a lado, cada um com sua API, sua fila e seu caderno. Isso vale até para a porta da API: ela só é liberada no último instante antes de o apisim subir, para que outra execução não a "roube" nesse meio-tempo.

O que o isolamento não cobre

O isolamento é lógico, não físico. Todas as execuções dividem:
- a mesma máquina (CPU, memória, disco);
- o mesmo servidor RabbitMQ, com filas separadas mas o mesmo broker.

Se você colocar paralelismo demais para a máquina, as execuções disputam recursos. Contagens como tentativas, sucessos e desistências provavelmente não mudam. Já métricas de tempo (latência, tempo até concluir) podem sair infladas.