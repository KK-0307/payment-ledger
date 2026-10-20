package bench

import (
	"context"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// apiProc is one API server subprocess. Running the real binary lets the
// benchmark SIGKILL it mid-request, exactly like a crashed container.
type apiProc struct {
	idx  int
	bin  string
	env  []string
	port int
	log  string

	mu      sync.Mutex
	cmd     *exec.Cmd
	exited  chan struct{}
	crashes int
}

// freePort picks a free port below the OS ephemeral range. An instance is
// restarted on the same port after a crash; an ephemeral port could be taken
// meanwhile by one of the benchmark's own outgoing connections.
func freePort() (int, error) {
	for i := 0; i < 200; i++ {
		port := 20000 + mrand.IntN(10000)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			ln.Close()
			return port, nil
		}
	}
	return 0, fmt.Errorf("no free port in 20000-29999")
}

func newAPIProc(idx int, bin, logDir string, env []string) (*apiProc, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	return &apiProc{idx: idx, bin: bin, port: port, log: filepath.Join(logDir, fmt.Sprintf("api-%d.log", idx)),
		// Copy: appending to the shared slice would let instances overwrite
		// each other's LISTEN_ADDR in its spare capacity.
		env: append(append([]string(nil), env...), fmt.Sprintf("LISTEN_ADDR=127.0.0.1:%d", port))}, nil
}

func (a *apiProc) URL() string { return fmt.Sprintf("http://127.0.0.1:%d", a.port) }

func (a *apiProc) start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	f, err := os.OpenFile(a.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command(a.bin)
	cmd.Env = a.env
	cmd.Stdout, cmd.Stderr = f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		return err
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); f.Close(); close(exited) }() //nolint:errcheck
	a.cmd, a.exited = cmd, exited
	return nil
}

func (a *apiProc) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	c := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		resp, err := c.Get(a.URL() + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.exitedChan():
			return fmt.Errorf("api %d exited during startup; see %s", a.idx, a.log)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("api %d not ready after %s; see %s", a.idx, timeout, a.log)
}

func (a *apiProc) exitedChan() chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.exited
}

// kill sends SIGKILL: no graceful shutdown, in-flight requests are lost.
// Returns the pid that was killed.
func (a *apiProc) kill() int {
	a.mu.Lock()
	cmd, exited := a.cmd, a.exited
	a.crashes++
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return 0
	}
	cmd.Process.Signal(syscall.SIGKILL) //nolint:errcheck
	<-exited
	return cmd.Process.Pid
}

// stop sends SIGTERM and waits for a graceful exit.
func (a *apiProc) stop() {
	a.mu.Lock()
	cmd, exited := a.cmd, a.exited
	a.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	select {
	case <-exited:
		return
	default:
	}
	cmd.Process.Signal(syscall.SIGTERM) //nolint:errcheck
	select {
	case <-exited:
	case <-time.After(45 * time.Second):
		cmd.Process.Signal(syscall.SIGKILL) //nolint:errcheck
		<-exited
	}
}

func (a *apiProc) running() bool {
	select {
	case <-a.exitedChan():
		return false
	default:
		return true
	}
}
