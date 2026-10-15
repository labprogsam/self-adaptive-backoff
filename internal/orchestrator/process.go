package orchestrator

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// freePort reserva uma porta livre no loopback. O listener é devolvido junto
// (ainda aberto) para que o chamador o feche só imediatamente antes de subir
// o processo que vai escutar nela, encolhendo ao mínimo a janela em que outra
// execução concorrente poderia roubar a mesma porta.
func freePort() (int, *net.TCPListener, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, nil, fmt.Errorf("erro ao reservar porta livre: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	return port, l.(*net.TCPListener), nil
}

func waitPortOpen(port int, timeout time.Duration) error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("porta %d não abriu em %s", port, timeout)
}

// buildEnv mescla o ambiente atual do processo com uma sequência de overrides
// (aplicados em ordem — o último vence), e serializa para o formato que
// exec.Cmd.Env espera. Mesclar em um map antes de serializar evita o efeito
// colateral de variáveis duplicadas no slice, cujo comportamento de
// precedência não é garantido da forma intuitiva por todas as libcs.
func buildEnv(overrides ...map[string]string) []string {
	merged := map[string]string{}
	for _, e := range os.Environ() {
		if i := strings.IndexByte(e, '='); i >= 0 {
			merged[e[:i]] = e[i+1:]
		}
	}
	for _, ov := range overrides {
		for k, v := range ov {
			merged[k] = v
		}
	}
	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}
	return out
}

// process é um processo filho em execução, com seu arquivo de log associado
// (nil quando os logs estão desativados — Close em *os.File nil é seguro).
type process struct {
	cmd     *exec.Cmd
	logFile *os.File
}

// startProcess inicia bin com env. Se logDir não for vazio, redireciona
// stdout+stderr para <logDir>/<name>.log (com paralelismo > 1, várias
// execuções escrevem ao mesmo tempo — misturar tudo no terminal seria
// ilegível); se for vazio, a saída do processo é descartada.
func startProcess(bin, logDir, name string, env []string) (*process, error) {
	cmd := exec.Command(bin)
	cmd.Env = env

	var logFile *os.File
	if logDir != "" {
		logPath := filepath.Join(logDir, name+".log")
		f, err := os.Create(logPath)
		if err != nil {
			return nil, fmt.Errorf("erro ao criar log %q: %w", logPath, err)
		}
		logFile = f
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("erro ao iniciar %q: %w", bin, err)
	}

	return &process{cmd: cmd, logFile: logFile}, nil
}

// stop envia SIGTERM e aguarda até graceTimeout; se o processo não sair
// sozinho, força com SIGKILL. Usado para apisim e consumer, que são
// servidores de vida longa (loop infinito) — diferente do producer, que
// termina sozinho depois de publicar e é esperado com cmd.Wait() direto.
func (p *process) stop(graceTimeout time.Duration) {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}

	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()

	_ = p.cmd.Process.Signal(syscall.SIGTERM)

	select {
	case <-done:
	case <-time.After(graceTimeout):
		_ = p.cmd.Process.Kill()
		<-done
	}

	p.logFile.Close()
}
