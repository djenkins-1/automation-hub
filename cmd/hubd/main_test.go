package main_test

// hubd is tested as a user runs it: the binary is built once, then started as
// a subprocess with real configs, signals and HTTP requests.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var hubd string // path to the built binary

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "hubd-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hubd = filepath.Join(dir, "hubd")
	if out, err := exec.Command("go", "build", "-o", hubd, ".").CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building hubd: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// env is the environment hubd runs with: the test's own, minus anything that
// would change which config is read or what it expands to, plus extra.
func env(extra ...string) []string {
	var out []string
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "HUB_") {
			out = append(out, kv)
		}
	}
	return append(out, extra...)
}

func writeFile(t *testing.T, path, content string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// runToExit runs hubd to completion and returns its exit code and stderr.
func runToExit(t *testing.T, dir string, environ []string, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, hubd, args...)
	cmd.Dir, cmd.Env = dir, environ
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("hubd %v did not exit; stderr:\n%s", args, stderr.String())
	}
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		t.Fatal(err)
	}
	return cmd.ProcessState.ExitCode(), stderr.String()
}

// process is a running hubd whose output is collected as it arrives.
type process struct {
	t    *testing.T
	cmd  *exec.Cmd
	done chan struct{}

	mu             sync.Mutex
	stdout, stderr []string
}

func start(t *testing.T, dir, config string, environ []string) *process {
	t.Helper()
	cmd := exec.Command(hubd, "-config", config)
	cmd.Dir, cmd.Env = dir, environ
	p := &process{t: t, cmd: cmd, done: make(chan struct{})}
	outPipe, _ := cmd.StdoutPipe()
	errPipe, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var readers sync.WaitGroup
	collect := func(r io.Reader, into *[]string) {
		defer readers.Done()
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			p.mu.Lock()
			*into = append(*into, sc.Text())
			p.mu.Unlock()
		}
	}
	readers.Add(2)
	go collect(outPipe, &p.stdout)
	go collect(errPipe, &p.stderr)
	go func() { readers.Wait(); _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.done
	})
	return p
}

func (p *process) lines(stdout bool) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if stdout {
		return append([]string(nil), p.stdout...)
	}
	return append([]string(nil), p.stderr...)
}

// waitFor returns the first line of stdout or stderr matching re.
func (p *process) waitFor(stdout bool, re string) []string {
	p.t.Helper()
	pat := regexp.MustCompile(re)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range p.lines(stdout) {
			if m := pat.FindStringSubmatch(l); m != nil {
				return m
			}
		}
		select {
		case <-p.done:
			p.t.Fatalf("hubd exited before printing %q; stderr:\n%s", re, strings.Join(p.lines(false), "\n"))
		case <-time.After(10 * time.Millisecond):
		}
	}
	p.t.Fatalf("timed out waiting for %q; stderr:\n%s", re, strings.Join(p.lines(false), "\n"))
	return nil
}

func (p *process) signal(s os.Signal) {
	p.t.Helper()
	if err := p.cmd.Process.Signal(s); err != nil {
		p.t.Fatal(err)
	}
}

// exited reports whether hubd has exited within d.
func (p *process) exited(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// addr is the address hubd says it listens on.
func (p *process) addr() string { return p.waitFor(false, `"api listening" addr=(\S+)`)[1] }

func post(t *testing.T, addr, token, body string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/events", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

const badConfig = "bogus: 1\n" // any config that fails to load names its own path

func TestTheConfigPathComesFromTheFlagThenTheEnvironmentThenTheDefault(t *testing.T) {
	dir := t.TempDir()
	flagCfg := writeFile(t, filepath.Join(dir, "from-flag.yaml"), badConfig)
	envCfg := writeFile(t, filepath.Join(dir, "from-env.yaml"), badConfig)
	writeFile(t, filepath.Join(dir, "hub.yaml"), badConfig)

	for _, tc := range []struct {
		name    string
		environ []string
		args    []string
		want    string
	}{
		{"flag wins", env("HUB_CONFIG=" + envCfg), []string{"-config", flagCfg}, "from-flag.yaml"},
		{"then HUB_CONFIG", env("HUB_CONFIG=" + envCfg), nil, "from-env.yaml"},
		{"then ./hub.yaml", env(), nil, "config hub.yaml:"},
	} {
		code, stderr := runToExit(t, dir, tc.environ, tc.args...)
		if code != 1 || !strings.Contains(stderr, tc.want) {
			t.Errorf("%s: exit %d, stderr %q; want exit 1 naming %q", tc.name, code, stderr, tc.want)
		}
	}
}

func TestAnUnexpectedArgumentIsAUsageError(t *testing.T) {
	if code, stderr := runToExit(t, t.TempDir(), env(), "serve"); code != 2 || !strings.Contains(stderr, "serve") {
		t.Fatalf("exit %d, stderr %q; want exit 2 naming the argument", code, stderr)
	}
}

func TestItRefusesToStartWithoutAToken(t *testing.T) {
	for name, tc := range map[string]struct {
		server  string
		environ []string
		want    string
	}{
		"no token":          {"server: { addr: \"127.0.0.1:0\" }", env(), "server.token is required"},
		"empty token":       {"server: { addr: \"127.0.0.1:0\", token: \"\" }", env(), "server.token is required"},
		"whitespace secret": {"server: { addr: \"127.0.0.1:0\", token: \"${HUB_TOKEN}\" }", env("HUB_TOKEN= \n"), "server.token is required"},
		"unset variable":    {"server: { addr: \"127.0.0.1:0\", token: \"${HUB_TOKEN}\" }", env(), "HUB_TOKEN"},
	} {
		dir := t.TempDir()
		cfg := writeFile(t, filepath.Join(dir, "hub.yaml"), "storage: { path: hub.db }\n"+tc.server+"\n")
		code, stderr := runToExit(t, dir, tc.environ, "-config", cfg)
		if code != 1 || !strings.Contains(stderr, tc.want) {
			t.Errorf("%s: exit %d, stderr %q; want exit 1 mentioning %q", name, code, stderr, tc.want)
		}
		if _, err := os.Stat(filepath.Join(dir, "hub.db")); err == nil {
			t.Errorf("%s: the database was created before the token was checked", name)
		}
	}
}

func TestAnAddressInUseIsAStartupError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	dir := t.TempDir()
	cfg := writeFile(t, filepath.Join(dir, "hub.yaml"), fmt.Sprintf(`
storage: { path: hub.db }
server: { addr: %q, token: t0ken }
`, ln.Addr().String()))
	if code, stderr := runToExit(t, dir, env(), "-config", cfg); code != 1 || !strings.Contains(stderr, "listen") {
		t.Fatalf("exit %d, stderr %q; want exit 1 explaining the listen failure", code, stderr)
	}
}

func TestItServesEventsAndStopsCleanlyOnSIGTERM(t *testing.T) {
	dir := t.TempDir()
	cfg := writeFile(t, filepath.Join(dir, "hub.yaml"), `
storage: { path: hub.db }
server: { addr: "127.0.0.1:0", token: "${HUB_TOKEN}" }
sinks: [{ name: console, type: stdout }]
rules: [{ name: all, actions: [{ sink: console, title: "{{.Event.Type}}", body: "{{.Event.Subject}}" }] }]
`)
	p := start(t, dir, cfg, env("HUB_TOKEN=s3cret"))
	addr := p.addr()

	if code := post(t, addr, "wrong", `{"source":"a","type":"b"}`); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d, want 401", code)
	}
	if code := post(t, addr, "s3cret", `{"source":"test","type":"ping","subject":"hello"}`); code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", code)
	}
	p.waitFor(true, `^notification: ping \| hello`)
	if _, err := os.Stat(filepath.Join(dir, "hub.db-wal")); err != nil {
		t.Fatalf("the database should be in WAL mode while running: %v", err)
	}

	p.signal(syscall.SIGTERM)
	if !p.exited(15 * time.Second) {
		t.Fatal("hubd did not stop after SIGTERM")
	}
	if code := p.cmd.ProcessState.ExitCode(); code != 0 {
		t.Fatalf("exit code %d after a clean shutdown, want 0; stderr:\n%s", code, strings.Join(p.lines(false), "\n"))
	}
	// Shutdown order: the server stops taking events, then the worker
	// drains, then the database closes.
	log := strings.Join(p.lines(false), "\n")
	order := []string{"shutdown requested", "queue worker stopped", "hubd stopped"}
	last := -1
	for _, msg := range order {
		i := strings.Index(log, msg)
		if i < last {
			t.Fatalf("log lines out of order or missing (want %q); stderr:\n%s", order, log)
		}
		last = i
	}
	if _, err := os.Stat(filepath.Join(dir, "hub.db-wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the WAL file should be gone after the database is closed cleanly (stat: %v)", err)
	}
}

func TestASecondSignalExitsAtOnceWhenTheDrainHangs(t *testing.T) {
	// A webhook that never answers keeps one delivery in flight, so the
	// worker's drain waits out its grace period.
	reached := make(chan struct{}, 1)
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case reached <- struct{}{}:
		default:
		}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(hang) }) // runs before srv.Close

	dir := t.TempDir()
	cfg := writeFile(t, filepath.Join(dir, "hub.yaml"), fmt.Sprintf(`
storage: { path: hub.db }
server: { addr: "127.0.0.1:0", token: t0ken }
sinks: [{ name: hook, type: http, url: %q }]
rules: [{ name: all, actions: [{ sink: hook, title: t }] }]
`, srv.URL))
	p := start(t, dir, cfg, env())
	if code := post(t, p.addr(), "t0ken", `{"source":"a","type":"b"}`); code != http.StatusAccepted {
		t.Fatalf("status %d, want 202", code)
	}
	select {
	case <-reached:
	case <-time.After(15 * time.Second):
		t.Fatal("the delivery never reached the webhook")
	}

	p.signal(syscall.SIGTERM)
	p.waitFor(false, "queue worker draining")
	if p.exited(time.Second) {
		t.Fatal("hubd exited without waiting for the in-flight delivery")
	}
	p.signal(syscall.SIGINT)
	if !p.exited(3 * time.Second) {
		t.Fatal("a second signal must end hubd at once")
	}
}
