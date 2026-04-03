package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort     = 8082
	webpageFile     = "webserver.html"
	temperatureFile = "testtemp.php"
	initialState    = "__initial__"
)

type serverState struct {
	mu               sync.Mutex
	webpageCache     []byte
	webpageMtimeNano int64
	powerState       PowerState
	activeStreamRefs int
}

var sharedState = &serverState{}

func (s *serverState) acquirePowerStateForStream() PowerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeStreamRefs++
	if s.activeStreamRefs == 1 || s.powerState == nil {
		s.powerState = NewPowerStateMQTT()
		logln("Created PowerStateMQTT for first active event stream")
	}
	return s.powerState
}

func (s *serverState) releasePowerStateForStream() {
	var stateToClose PowerState
	s.mu.Lock()
	if s.activeStreamRefs == 0 {
		s.mu.Unlock()
		return
	}
	s.activeStreamRefs--
	if s.activeStreamRefs == 0 {
		stateToClose = s.powerState
		s.powerState = nil
		logln("No active event streams remain; closing PowerStateMQTT")
	}
	s.mu.Unlock()

	if stateToClose != nil {
		stateToClose.Close()
	}
}

func (s *serverState) currentPowerState() PowerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.powerState
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

func serveMakeGraph(w http.ResponseWriter, r *http.Request) {
	logf("Received request for %s\n", r.URL.Path)
	ptsFile, err := os.CreateTemp(".", "dpt_*.tsv")
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not create temp points file: %v", err), http.StatusInternalServerError)
		return
	}
	ptsPath := ptsFile.Name()
	logf("Using temp points file: %s\n", ptsPath)
	_ = ptsFile.Close()
	defer os.Remove(ptsPath)

	gphFile, err := os.CreateTemp(".", "gph_*.png")
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not create temp graph file: %v", err), http.StatusInternalServerError)
		return
	}
	gphPath := gphFile.Name()
	logf("Using temp graph file: %s\n", gphPath)
	_ = gphFile.Close()
	defer os.Remove(gphPath)

	workDir := "."
	cmdCtx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	getPtsCmd := exec.CommandContext(cmdCtx, "./getpts.sh", ptsPath)
	getPtsCmd.Dir = workDir
	output, cmdErr := getPtsCmd.CombinedOutput()
	if cmdErr != nil {
		http.Error(w, fmt.Sprintf("getpts.sh failed: %v\n%s", cmdErr, strings.TrimSpace(string(output))), http.StatusInternalServerError)
		return
	}

	if len(output) > 0 {
		if writeErr := os.WriteFile(ptsPath, output, 0o644); writeErr != nil {
			http.Error(w, fmt.Sprintf("Could not write points output to %s: %v", ptsPath, writeErr), http.StatusInternalServerError)
			return
		}
	}

	if fileSize(ptsPath) <= 0 {
		http.Error(w, "getpts.sh produced no point data", http.StatusInternalServerError)
		return
	}
	if err := appendEmphasizedFinalPoint(cmdCtx, ptsPath); err != nil {
		http.Error(w, fmt.Sprintf("Could not emphasize final point data: %v", err), http.StatusInternalServerError)
		return
	}
	logf("Points file generated successfully at %s. Size: %d\n", ptsPath, fileSize(ptsPath))

	tempScriptPath := filepath.Join(workDir, "tempscript")
	gnuplotExpr := fmt.Sprintf("filename='%s';ofilename='%s'", ptsPath, gphPath)
	gnuplotCmd := exec.CommandContext(cmdCtx, "gnuplot", "-e", gnuplotExpr, tempScriptPath)
	gnuplotCmd.Dir = workDir
	if output, cmdErr := gnuplotCmd.CombinedOutput(); cmdErr != nil {
		http.Error(w, fmt.Sprintf("gnuplot failed: %v\n%s", cmdErr, strings.TrimSpace(string(output))), http.StatusInternalServerError)
		return
	}
	content, err := os.ReadFile(gphPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not read generated graph: %v", err), http.StatusInternalServerError)
		return
	}
	logf("image file of length %d generated successfully\n", len(content))

	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func streamEvents(w http.ResponseWriter, r *http.Request) {
	remotePort := "unknown"
	if idx := strings.LastIndex(r.RemoteAddr, ":"); idx >= 0 && idx < len(r.RemoteAddr)-1 {
		remotePort = r.RemoteAddr[idx+1:]
	}

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

	logf("[%s] Opened event stream\n", remotePort)
	lastState := ""
	heartbeatInterval := 20 * time.Second

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Closed event stream\n", remotePort)
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
			logf("[%s] Closed event stream\n", remotePort)
			return
		case state = <-stateCh:
			cancelWait()
		}

		if state != lastState {
			payload, _ := json.Marshal(map[string]string{"state": state})
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				logf("[%s] Closed event stream\n", remotePort)
				return
			}
			flusher.Flush()
			lastState = state
			logf("[%s] Sent event: %s\n", remotePort, payload)
		} else {
			if _, err := ioWriteString(w, ": keepalive\n\n"); err != nil {
				logf("[%s] Closed event stream\n", remotePort)
				return
			}
			flusher.Flush()
		}
	}
}

func handlePower(w http.ResponseWriter, r *http.Request) {
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

	logf("Setting state to %s\n", value)
	powerState := sharedState.currentPowerState()
	if powerState == nil {
		writeJSON(w, map[string]string{"error": "No active event stream"}, http.StatusServiceUnavailable)
		return
	}

	powerState.RequestStateChange(value)
	time.Sleep(100 * time.Millisecond)
	writeJSON(w, map[string]string{"state": value}, http.StatusOK)
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

func appendEmphasizedFinalPoint(ctx context.Context, path string) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", `PAD=$(tail -n 1 "$ptsfile" | awk -v OFS="\t" '{ print 120, $2, $3 }'); printf '%s\n' "$PAD" >> "$ptsfile"`)
	cmd.Env = append(os.Environ(), "ptsfile="+path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("append final point failed: %v: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
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
	http.HandleFunc("/api/power", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		handlePower(w, r)
	})

	addr := fmt.Sprintf("%s:%d", *host, *port)
	logf("Serving on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		logln("Server stopped:", err)
	}
}
