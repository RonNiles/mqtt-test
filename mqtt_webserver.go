package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultPort  = 8082
	webpageFile  = "webserver.html"
	initialState = "__initial__"
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
		fmt.Println("Created PowerStateMQTT for first active event stream")
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
		fmt.Println("No active event streams remain; closing PowerStateMQTT")
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

	fmt.Printf("[%s] Opened event stream\n", remotePort)
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
			fmt.Printf("[%s] Closed event stream\n", remotePort)
			return
		case state = <-stateCh:
			cancelWait()
		}

		if state != lastState {
			payload, _ := json.Marshal(map[string]string{"state": state})
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				fmt.Printf("[%s] Closed event stream\n", remotePort)
				return
			}
			flusher.Flush()
			lastState = state
			fmt.Printf("[%s] Sent event: %s\n", remotePort, payload)
		} else {
			if _, err := ioWriteString(w, ": keepalive\n\n"); err != nil {
				fmt.Printf("[%s] Closed event stream\n", remotePort)
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

	fmt.Printf("Setting state to %s\n", value)
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
	fmt.Printf("Serving on %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println("Server stopped:", err)
	}
}
