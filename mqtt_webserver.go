package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultPort     = 8082
	webpageFile     = "webserver.html"
	temperatureFile = "testtemp.php"
	initialState    = "__initial__"
)

type clientActivity struct {
	lastMakeGraphCall    time.Time
	waitForChangeSeconds int
	lastGraphVersion     int
}

type serverState struct {
	mu               sync.Mutex
	webpageCache     []byte
	webpageMtimeNano int64
	powerState       PowerState
	activeStreamRefs int
	transientRefs    int
	transientTimer   *time.Timer
	streamTimer      *time.Timer
	clientActivities map[string]*clientActivity // IP -> client activity
}

var sharedState = &serverState{}

func (s *serverState) lockedGetClientActivity(ip string) *clientActivity {
	if s.clientActivities == nil {
		s.clientActivities = make(map[string]*clientActivity)
	}
	if _, exists := s.clientActivities[ip]; !exists {
		s.clientActivities[ip] = &clientActivity{}
	}
	return s.clientActivities[ip]
}

func (s *serverState) recordMakeGraphCall(ip string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	activity := s.lockedGetClientActivity(ip)
	activity.waitForChangeSeconds = 0
	activity.lastMakeGraphCall = time.Now()
}

func (s *serverState) shouldHangupWaitForChange(ip string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	activity := s.lockedGetClientActivity(ip)
	activity.waitForChangeSeconds += seconds
	logf("Checking if client %s should be hung up: %d seconds\n", ip, activity.waitForChangeSeconds)
	return activity.waitForChangeSeconds > 180
}

func (s *serverState) acquirePowerStateForStream() PowerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeStreamRefs++
	if s.transientTimer != nil {
		s.transientTimer.Stop()
		s.transientTimer = nil
	}
	if s.streamTimer != nil {
		s.streamTimer.Stop()
		s.streamTimer = nil
	}
	if s.powerState == nil {
		s.powerState = NewPowerStateMQTT()
		logln("Created PowerStateMQTT for first active event stream")
	} else if s.activeStreamRefs == 1 {
		logln("Reusing existing PowerStateMQTT for first active event stream")
	}
	return s.powerState
}

func (s *serverState) releasePowerStateForStream() {
	s.mu.Lock()
	if s.activeStreamRefs == 0 {
		s.mu.Unlock()
		return
	}
	s.activeStreamRefs--
	if s.activeStreamRefs == 0 && s.transientRefs == 0 && s.powerState != nil {
		logln("No active event streams remain; starting 30s grace period")
		s.streamTimer = time.AfterFunc(30*time.Second, func() {
			s.mu.Lock()
			if s.activeStreamRefs > 0 || s.transientRefs > 0 {
				s.mu.Unlock()
				return
			}
			ps := s.powerState
			s.powerState = nil
			s.streamTimer = nil
			s.mu.Unlock()
			if ps != nil {
				ps.Close()
				logln("Closed idle PowerStateMQTT after 30s stream grace period")
			}
		})
	}
	s.mu.Unlock()
}

func (s *serverState) currentPowerState() PowerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.powerState
}

// acquirePowerStateForRequest returns the active stream power state if one exists,
// otherwise creates (or reuses) a shared PowerStateMQTT and increments the transient
// ref count, returning a flag indicating the caller must release the transient ref.
func (s *serverState) acquirePowerStateForRequest() (PowerState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeStreamRefs > 0 {
		return s.powerState, false
	}
	if s.transientTimer != nil {
		s.transientTimer.Stop()
		s.transientTimer = nil
	}
	if s.streamTimer != nil {
		s.streamTimer.Stop()
		s.streamTimer = nil
	}
	s.transientRefs++
	if s.powerState == nil {
		s.powerState = NewPowerStateMQTT()
		logln("Created PowerStateMQTT for wait_for_change request")
	}
	return s.powerState, true
}

// releasePowerStateForRequest decrements the transient ref count. When it reaches
// zero (with no active streams), a 30-second idle timer is started; if no new
// request arrives the PowerStateMQTT is closed.
func (s *serverState) releasePowerStateForRequest(transient bool) {
	if !transient {
		return
	}
	s.mu.Lock()
	s.transientRefs--
	if s.transientRefs == 0 && s.activeStreamRefs == 0 && s.powerState != nil {
		s.transientTimer = time.AfterFunc(30*time.Second, func() {
			s.mu.Lock()
			if s.transientRefs > 0 || s.activeStreamRefs > 0 {
				s.mu.Unlock()
				return
			}
			ps := s.powerState
			s.powerState = nil
			s.transientTimer = nil
			s.mu.Unlock()
			if ps != nil {
				ps.Close()
				logln("Closed idle PowerStateMQTT after 30s idle")
			}
		})
	}
	s.mu.Unlock()
}

func serveRoot(w http.ResponseWriter) {
	filePath := filepath.Join(".", webpageFile)
	stat, err := os.Stat(filePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not load %s: %v", filePath, err), http.StatusInternalServerError)
		return
	}

	sharedState.mu.Lock()
	if sharedState.webpageCache == nil || sharedState.webpageMtimeNano != stat.ModTime().UnixNano() {
		content, readErr := os.ReadFile(filePath)
		if readErr != nil {
			sharedState.mu.Unlock()
			http.Error(w, fmt.Sprintf("Could not load %s: %v", filePath, readErr), http.StatusInternalServerError)
			return
		}
		sharedState.webpageCache = content
		sharedState.webpageMtimeNano = stat.ModTime().UnixNano()
	}
	content := append([]byte(nil), sharedState.webpageCache...)
	sharedState.mu.Unlock()

	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func serveTemperature(w http.ResponseWriter) {
	filePath := filepath.Join(".", temperatureFile)
	content, err := os.ReadFile(filePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not load %s: %v", filePath, err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

type graphState struct {
	mu          sync.Mutex
	version     int
	pngData     []byte
	lastErr     error
	waiters     []chan struct{}
	running     bool
	lastRequest time.Time
}

var sharedGraphState = &graphState{}

func generateGraphOnce(ctx context.Context) ([]byte, int, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	cmd1 := exec.CommandContext(cmdCtx, "./getpts.sh")
	cmd1.Dir = "."
	ptsOutput, err := cmd1.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(ee.Stderr))
		}
		if stderr != "" {
			return nil, 0, fmt.Errorf("getpts.sh failed: %v: %s", err, stderr)
		}
		return nil, 0, fmt.Errorf("getpts.sh failed: %v", err)
	}

	lastAge, err := mostRecentRowAge(ptsOutput)
	if err != nil {
		return nil, 0, fmt.Errorf("parsing getpts.sh output: %v", err)
	}

	ptsOutput, err = appendFinalPTSRow(ptsOutput)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid getpts.sh output: %v", err)
	}

	tempscriptContent, err := os.ReadFile(filepath.Join(".", "tempscript"))
	if err != nil {
		return nil, 0, fmt.Errorf("could not read tempscript: %v", err)
	}

	gnuplotInput := fmt.Sprintf("$DATA << EOD\n%s\nEOD\n%s", ptsOutput, tempscriptContent)
	cmd2 := exec.CommandContext(cmdCtx, "gnuplot")
	cmd2.Dir = "."
	cmd2.Stdin = strings.NewReader(gnuplotInput)
	content, err := cmd2.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = strings.TrimSpace(string(ee.Stderr))
		}
		if stderr != "" {
			return nil, 0, fmt.Errorf("graph generation failed: %v: %s", err, stderr)
		}
		return nil, 0, fmt.Errorf("graph generation failed: %v", err)
	}
	if len(content) == 0 {
		return nil, 0, fmt.Errorf("graph generation produced no output")
	}
	return content, lastAge, nil
}

// mostRecentRowAge parses the first column (negative seconds-from-now) of the
// last data row in getpts.sh output. Returns 0 if it can't parse.
func mostRecentRowAge(data []byte) (int, error) {
	trimmed := bytes.TrimRight(data, "\r\n")
	if len(trimmed) == 0 {
		return 0, fmt.Errorf("empty output")
	}
	lines := bytes.Split(trimmed, []byte("\n"))
	lastLine := lines[len(lines)-1]
	fields := bytes.SplitN(lastLine, []byte("\t"), 2)
	if len(fields) < 1 {
		return 0, fmt.Errorf("no fields in final row")
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(fields[0])))
	if err != nil {
		return 0, fmt.Errorf("first column not an int: %v", err)
	}
	return v, nil
}

func generatorLoop() {
	for {
		start := time.Now()
		pngData, lastAge, err := generateGraphOnce(context.Background())
		elapsed := time.Since(start)

		sharedGraphState.mu.Lock()
		if err == nil {
			sharedGraphState.version++
			sharedGraphState.pngData = pngData
			sharedGraphState.lastErr = nil
			logf("generator: produced version %d (lastAge=%ds, took %v, %d bytes)\n",
				sharedGraphState.version, lastAge, elapsed.Round(time.Millisecond), len(pngData))
		} else {
			sharedGraphState.lastErr = err
			logf("generator: generation failed: %v\n", err)
		}
		waiters := sharedGraphState.waiters
		sharedGraphState.waiters = nil

		idle := time.Since(sharedGraphState.lastRequest) > 60*time.Second
		if idle {
			sharedGraphState.running = false
			sharedGraphState.mu.Unlock()
			for _, ch := range waiters {
				close(ch)
			}
			logln("generator: idle for >60s, exiting")
			return
		}
		sharedGraphState.mu.Unlock()

		for _, ch := range waiters {
			close(ch)
		}

		var sleep time.Duration
		if err != nil {
			sleep = 5 * time.Second
		} else {
			sleep = time.Duration(11+lastAge) * time.Second
			if sleep < 2*time.Second {
				sleep = 2 * time.Second
			}
			if sleep > 11*time.Second {
				sleep = 11 * time.Second
			}
		}
		time.Sleep(sleep)
	}
}

func serveMakeGraph(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.RemoteAddr
	logf("[%s] Received request for %s\n", remoteAddr, r.URL.Path)
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		ip = remoteAddr
	}

	// reset waitForChangeSeconds (preserve original side effect)
	sharedState.recordMakeGraphCall(ip)

	sharedState.mu.Lock()
	activity := sharedState.lockedGetClientActivity(ip)
	knownVersion := activity.lastGraphVersion
	sharedState.mu.Unlock()

	sharedGraphState.mu.Lock()
	sharedGraphState.lastRequest = time.Now()
	currentVersion := sharedGraphState.version
	currentData := sharedGraphState.pngData
	haveData := currentData != nil
	serveImmediately := haveData && knownVersion != 0 && knownVersion != currentVersion

	if serveImmediately {
		sharedGraphState.mu.Unlock()
		sharedState.mu.Lock()
		activity.lastGraphVersion = currentVersion
		sharedState.mu.Unlock()
		writePNG(w, remoteAddr, currentVersion, currentData)
		return
	}

	waiter := make(chan struct{})
	sharedGraphState.waiters = append(sharedGraphState.waiters, waiter)
	if !sharedGraphState.running {
		sharedGraphState.running = true
		go generatorLoop()
		logf("[%s] Started generator goroutine\n", remoteAddr)
	}
	sharedGraphState.mu.Unlock()

	select {
	case <-waiter:
	case <-r.Context().Done():
		logf("[%s] Client disconnected while waiting for graph\n", remoteAddr)
		return
	}

	sharedGraphState.mu.Lock()
	data := sharedGraphState.pngData
	newVersion := sharedGraphState.version
	lastErr := sharedGraphState.lastErr
	sharedGraphState.mu.Unlock()

	if data == nil {
		msg := "graph generation failed"
		if lastErr != nil {
			msg = fmt.Sprintf("graph generation failed: %v", lastErr)
		}
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	sharedState.mu.Lock()
	activity.lastGraphVersion = newVersion
	sharedState.mu.Unlock()

	writePNG(w, remoteAddr, newVersion, data)
}

func writePNG(w http.ResponseWriter, remoteAddr string, version int, data []byte) {
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
	logf("[%s] Served graph version %d (%d bytes)\n", remoteAddr, version, len(data))
}

func streamEvents(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.RemoteAddr

	powerState := sharedState.acquirePowerStateForStream()
	defer sharedState.releasePowerStateForStream()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	logf("[%s] Opened event stream\n", remoteAddr)
	lastState := ""
	heartbeatInterval := 20 * time.Second

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Closed event stream\n", remoteAddr)
			return
		default:
		}

		fromState := initialState
		if lastState != "" {
			fromState = lastState
		}

		waitCtx, cancelWait := context.WithCancel(ctx)
		stateCh := make(chan string, 1)
		go func(from string) {
			stateCh <- powerState.WaitForChange(waitCtx, from, heartbeatInterval)
		}(fromState)

		var state string
		select {
		case <-ctx.Done():
			cancelWait()
			logf("[%s] Closed event stream\n", remoteAddr)
			return
		case state = <-stateCh:
			cancelWait()
		}

		if state != lastState {
			payload, _ := json.Marshal(map[string]string{"state": state})
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				logf("[%s] Closed event stream\n", remoteAddr)
				return
			}
			flusher.Flush()
			lastState = state
			logf("[%s] Sent event: %s\n", remoteAddr, payload)
		} else {
			if _, err := ioWriteString(w, ": keepalive\n\n"); err != nil {
				logf("[%s] Closed event stream\n", remoteAddr)
				return
			}
			flusher.Flush()
		}
	}
}

func handleWaitForChange(w http.ResponseWriter, r *http.Request) {
	fromState := r.URL.Query().Get("state")
	if fromState == "" {
		fromState = initialState
	}

	intervalSecs := 30
	if s := r.URL.Query().Get("interval"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			intervalSecs = v
		}
	}

	remoteAddr := r.RemoteAddr

	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		ip = remoteAddr
	}
	if sharedState.shouldHangupWaitForChange(ip, intervalSecs) {
		logf("[%s] wait_for_change: no recent makegraph.activity, resetting connection\n", remoteAddr)
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, hijackErr := hijacker.Hijack(); hijackErr == nil {
				if tc, ok2 := conn.(*net.TCPConn); ok2 {
					_ = tc.SetLinger(0)
				}
				_ = conn.Close()
				return
			}
		}
		http.Error(w, "No recent makegraph.php activity", http.StatusServiceUnavailable)
		return
	}

	logf("[%s] Received wait_for_change request: from=%q, interval=%d\n", remoteAddr, fromState, intervalSecs)
	powerState, transient := sharedState.acquirePowerStateForRequest()
	defer sharedState.releasePowerStateForRequest(transient)

	newState := powerState.WaitForChange(r.Context(), fromState, time.Duration(intervalSecs)*time.Second)

	logf("[%s] wait_for_change: %q -> %q\n", remoteAddr, fromState, newState)
	writeJSON(w, map[string]string{"state": newState}, http.StatusOK)
}

func handlePower(w http.ResponseWriter, r *http.Request) {
	remoteAddr := r.RemoteAddr
	var body bytes.Buffer
	_, _ = body.ReadFrom(r.Body)
	if body.Len() == 0 {
		body.WriteString("{}")
	}

	var data struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body.Bytes(), &data); err != nil {
		writeJSON(w, map[string]string{"error": "Invalid JSON body"}, http.StatusBadRequest)
		return
	}

	value := strings.ToLower(strings.TrimSpace(data.Value))
	switch value {
	case "on", "off", "disconnected", "loading":
	default:
		writeJSON(w, map[string]string{"error": "value must be on, off, disconnected, or loading"}, http.StatusBadRequest)
		return
	}

	logf("[%s] Setting state to %s\n", remoteAddr, value)
	powerState := sharedState.currentPowerState()
	if powerState == nil {
		writeJSON(w, map[string]string{"error": "No active event stream"}, http.StatusServiceUnavailable)
		return
	}

	powerState.RequestStateChange(value)
	writeJSON(w, map[string]string{"state": value}, http.StatusOK)
}

func handleTurn(w http.ResponseWriter, r *http.Request, state string, bodyText string) {
	remoteAddr := r.RemoteAddr
	status := http.StatusOK
	responseBody := bodyText
	host, tasmotaID := "", ""

	if host = strings.TrimSpace(os.Getenv("MQTT_HOST")); host == "" {
		host = "127.0.0.1"
	}
	if tasmotaID = strings.TrimSpace(os.Getenv("TASMOTA_ID")); tasmotaID == "" {
		tasmotaID = "XXXXXX"
	}
	payload := strings.ToUpper(state)
	topic := fmt.Sprintf("cmnd/tasmota_%s/POWER", tasmotaID)
	logf("[%s] Publishing MQTT %s to %s\n", remoteAddr, payload, topic)
	cmd := exec.Command("mosquitto_pub", "-h", host, "-t", topic, "-m", payload)
	if output, err := cmd.CombinedOutput(); err != nil {
		status = http.StatusInternalServerError
		trimmed := strings.TrimSpace(string(output))
		if trimmed == "" {
			responseBody = fmt.Sprintf("Error publishing MQTT command: %v", err)
		} else {
			responseBody = fmt.Sprintf("Error publishing MQTT command: %v (%s)", err, trimmed)
		}
	}

	head := `<head><meta http-equiv="refresh" content="1; URL=testtemp.php">` +
		`<meta name="keywords" content="automatic redirection"></head>`
	content := []byte("<html>" + head + "<body>" + responseBody + "</body></html>")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.WriteHeader(status)
	_, _ = w.Write(content)
}

func writeJSON(w http.ResponseWriter, data map[string]string, status int) {
	resp, _ := json.Marshal(data)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(resp)))
	w.WriteHeader(status)
	_, _ = w.Write(resp)
}

func ioWriteString(w http.ResponseWriter, s string) (int, error) {
	return w.Write([]byte(s))
}

func appendFinalPTSRow(data []byte) ([]byte, error) {
	trimmed := bytes.TrimRight(data, "\r\n")
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("empty output")
	}

	lines := bytes.Split(trimmed, []byte("\n"))
	lastLine := lines[len(lines)-1]
	fields := bytes.SplitN(lastLine, []byte("\t"), 3)
	if len(fields) != 3 {
		return nil, fmt.Errorf("expected 3 tab-separated columns in final row")
	}

	paddedLine := append([]byte("120\t"), fields[1]...)
	paddedLine = append(paddedLine, '\t')
	paddedLine = append(paddedLine, fields[2]...)

	result := append([]byte(nil), trimmed...)
	result = append(result, '\n')
	result = append(result, paddedLine...)
	result = append(result, '\n')
	return result, nil
}

func main() {
	host := flag.String("host", "127.0.0.1", "Host/IP address to bind the web server to")
	port := flag.Int("port", defaultPort, "Port to run the web server on")
	flag.Parse()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			serveRoot(w)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/testtemp.php", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testtemp.php" {
			serveTemperature(w)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/makegraph.php", func(w http.ResponseWriter, r *http.Request) {
		logf("Received request for %s\n", r.URL.Path)
		if r.URL.Path == "/makegraph.php" {
			serveMakeGraph(w, r)
			return
		}
		http.NotFound(w, r)
	})
	http.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		streamEvents(w, r)
	})
	http.HandleFunc("/api/wait_for_change", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		handleWaitForChange(w, r)
	})
	http.HandleFunc("/api/power", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		handlePower(w, r)
	})
	http.HandleFunc("/turnon.php", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/turnon.php" {
			http.NotFound(w, r)
			return
		}
		handleTurn(w, r, "on", "Turned pump on")
	})
	http.HandleFunc("/turnoff.php", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/turnoff.php" {
			http.NotFound(w, r)
			return
		}
		handleTurn(w, r, "off", "Turned pump off")
	})

	addr := fmt.Sprintf("%s:%d", *host, *port)
	srv := &http.Server{Addr: addr}

	go func() {
		logf("Serving on %s\n", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logln("Server stopped:", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logf("Received signal %v, shutting down...\n", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logln("HTTP shutdown error:", err)
	}

	sharedState.mu.Lock()
	ps := sharedState.powerState
	sharedState.powerState = nil
	if sharedState.transientTimer != nil {
		sharedState.transientTimer.Stop()
	}
	if sharedState.streamTimer != nil {
		sharedState.streamTimer.Stop()
	}
	sharedState.mu.Unlock()
	if ps != nil {
		ps.Close()
		logln("Closed PowerStateMQTT during shutdown")
	}

	logln("Shutdown complete")
}
