package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	brokerKeepAlive = 60 * time.Second
	clientIDPrefix  = "on-off-controller"
)

type PowerState interface {
	WaitForChange(ctx context.Context, fromState string, timeout time.Duration) string
	RequestStateChange(newState string)
	Close()
}

type PowerStateEmulator struct {
	mu       sync.Mutex
	notifyCh chan struct{}
	state    string
	stopCh   chan struct{}
}

func NewPowerStateEmulator() *PowerStateEmulator {
	p := &PowerStateEmulator{
		notifyCh: make(chan struct{}),
		state:    "disconnected",
		stopCh:   make(chan struct{}),
	}
	go p.connectionWatchdog()
	return p
}

func (p *PowerStateEmulator) WaitForChange(ctx context.Context, fromState string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		p.mu.Lock()
		state := p.state
		if state != fromState {
			p.mu.Unlock()
			return state
		}
		ch := p.notifyCh
		remaining := time.Until(deadline)
		p.mu.Unlock()

		if remaining <= 0 {
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		}

		select {
		case <-ch:
		case <-time.After(remaining):
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		case <-ctx.Done():
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		}
	}
}

func (p *PowerStateEmulator) RequestStateChange(newState string) {
	p.mu.Lock()
	if p.state == "loading" {
		p.mu.Unlock()
		return
	}
	if newState == p.state {
		p.mu.Unlock()
		return
	}
	p.state = "loading"
	p.signalLocked()
	p.mu.Unlock()

	timer := time.NewTimer(1 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		p.mu.Lock()
		if p.state == "loading" {
			p.state = newState
			p.signalLocked()
		}
		p.mu.Unlock()
	case <-p.stopCh:
	}
}

func (p *PowerStateEmulator) Close() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}

func (p *PowerStateEmulator) connectionWatchdog() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.mu.Lock()
			if p.state == "disconnected" {
				logln("connection_watchdog: No reconnection detected after 10 seconds, simulating reconnection...")
				p.state = "loading"
				p.signalLocked()
				p.mu.Unlock()

				time.Sleep(1 * time.Second)

				p.mu.Lock()
				p.state = "on"
				p.signalLocked()
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()
		case <-p.stopCh:
			return
		}
	}
}

func (p *PowerStateEmulator) signalLocked() {
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
}

type PowerStateMQTT struct {
	clientID string

	host    string
	port    int
	tasmota string

	lwtTopic          string
	powerTopic        string
	powerCommandTopic string

	mu              sync.Mutex
	notifyCh        chan struct{}
	state           string
	disconnectCount int
	loadingCount    int
	client          mqtt.Client

	stopCh chan struct{}
	doneCh chan struct{}
}

func NewPowerStateMQTT() *PowerStateMQTT {
	tasmotaID := getenv("TASMOTA_ID", "XXXXXX")
	hostname, _ := os.Hostname()
	defaultClientID := fmt.Sprintf("%s-%s-%d", clientIDPrefix, strings.ReplaceAll(hostname, " ", "-"), os.Getpid())
	p := &PowerStateMQTT{
		clientID:          getenv("MQTT_CLIENT_ID", defaultClientID),
		host:              getenv("MQTT_HOST", "localhost"),
		port:              atoiDefault(getenv("MQTT_PORT", "1883"), 1883),
		tasmota:           tasmotaID,
		lwtTopic:          fmt.Sprintf("tele/tasmota_%s/LWT", tasmotaID),
		powerTopic:        fmt.Sprintf("stat/tasmota_%s/POWER", tasmotaID),
		powerCommandTopic: fmt.Sprintf("cmnd/tasmota_%s/POWER", tasmotaID),
		notifyCh:          make(chan struct{}),
		state:             "disconnected",
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}
	go p.mqttManager()
	return p
}

func (p *PowerStateMQTT) WaitForChange(ctx context.Context, fromState string, timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for {
		p.mu.Lock()
		state := p.state
		if state != fromState {
			p.mu.Unlock()
			return state
		}
		ch := p.notifyCh
		remaining := time.Until(deadline)
		p.mu.Unlock()

		if remaining <= 0 {
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		}

		select {
		case <-ch:
		case <-time.After(remaining):
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		case <-ctx.Done():
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		case <-p.stopCh:
			p.mu.Lock()
			state = p.state
			p.mu.Unlock()
			return state
		}
	}
}

func (p *PowerStateMQTT) RequestStateChange(newState string) {
	p.mu.Lock()
	if p.state != "on" && p.state != "off" {
		logf("request_state_change: Ignoring state change request to '%s' because current state is '%s'\n", newState, p.state)
		p.mu.Unlock()
		return
	}

	p.state = "loading"
	p.loadingCount++
	p.signalLocked()
	client := p.client
	topic := p.powerCommandTopic
	p.mu.Unlock()

	if (newState == "on" || newState == "off") && client != nil {
		token := client.Publish(topic, 0, false, strings.ToUpper(newState))
		if token.WaitTimeout(5*time.Second) && token.Error() == nil {
			logf("request_state_change: Published MQTT message to change state to '%s'\n", newState)
			return
		}
		if err := token.Error(); err != nil {
			logln("Failed to publish MQTT message:", err)
		}
	}

	p.cleanupClient()
	p.mu.Lock()
	p.state = "disconnected"
	p.disconnectCount++
	logln("request_state_change: Failed to publish MQTT message, setting state to 'disconnected'")
	p.signalLocked()
	p.mu.Unlock()
}

func (p *PowerStateMQTT) Close() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
	<-p.doneCh
}

func (p *PowerStateMQTT) mqttManager() {
	defer close(p.doneCh)

	p.mu.Lock()
	p.state = "loading"
	p.loadingCount++
	logln("mqtt_manager: state set to 'loading', count:", p.loadingCount)
	p.signalLocked()
	p.mu.Unlock()

	needReconnect := true
	needDisconnect := false

	for {
		select {
		case <-p.stopCh:
			p.cleanupClient()
			return
		default:
		}

		if needReconnect {
			logln("mqtt_manager: Attempting to start MQTT client...")
			p.startClient()
			needReconnect = false
		}

		if needDisconnect {
			logln("mqtt_manager: Attempting to clean up MQTT client...")
			p.cleanupClient()
			p.mu.Lock()
			p.state = "disconnected"
			p.disconnectCount++
			p.signalLocked()
			p.mu.Unlock()
			needDisconnect = false
		}

		p.mu.Lock()
		prevState := p.state
		prevDisconnectCount := p.disconnectCount
		prevLoadingCount := p.loadingCount
		waitCh := p.notifyCh
		p.mu.Unlock()

		select {
		case <-waitCh:
		case <-time.After(10 * time.Second):
		case <-p.stopCh:
			p.cleanupClient()
			return
		}

		p.mu.Lock()
		if p.state == "disconnected" && prevState == "disconnected" && p.disconnectCount == prevDisconnectCount {
			logf("mqtt_manager: Detected prolonged disconnected state (count %d), will attempt to reconnect...\n", p.disconnectCount)
			p.state = "loading"
			p.loadingCount++
			p.signalLocked()
			needReconnect = true
			p.mu.Unlock()
			continue
		}
		if p.state == "loading" && prevState == "loading" && p.loadingCount == prevLoadingCount {
			logf("mqtt_manager: Detected prolonged loading state (count %d), disconnecting\n", p.loadingCount)
			needDisconnect = true
			p.mu.Unlock()
			continue
		}
		p.mu.Unlock()
	}
}

func (p *PowerStateMQTT) startClient() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", p.host, p.port))
	opts.SetClientID(p.clientID)
	opts.SetKeepAlive(brokerKeepAlive)
	opts.SetAutoReconnect(false)
	opts.SetOnConnectHandler(func(c mqtt.Client) {
		if token := c.Subscribe(p.lwtTopic, 0, nil); token.Wait() && token.Error() != nil {
			logln("Subscribe error for LWT topic:", token.Error())
		}
		if token := c.Subscribe(p.powerTopic, 0, nil); token.Wait() && token.Error() != nil {
			logln("Subscribe error for POWER topic:", token.Error())
		}
		logf("Subscribed to MQTT topics: %s, %s\n", p.lwtTopic, p.powerTopic)
		c.Publish(p.powerCommandTopic, 0, false, "")
	})
	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		p.mu.Lock()
		if p.client != c {
			p.mu.Unlock()
			logln("Received on_disconnect for an old client, ignoring. Reason:", err)
			return
		}
		p.state = "disconnected"
		p.disconnectCount++
		logln("state set to 'disconnected' in on_disconnect. Reason:", err)
		p.signalLocked()
		p.mu.Unlock()
	})
	opts.SetDefaultPublishHandler(func(c mqtt.Client, msg mqtt.Message) {
		p.onMessage(c, msg)
	})

	client := mqtt.NewClient(opts)
	p.mu.Lock()
	p.client = client
	p.mu.Unlock()

	logf("Attempting to connect to MQTT broker at %s:%d...\n", p.host, p.port)
	connectStart := time.Now()
	token := client.Connect()
	if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		elapsed := time.Since(connectStart).Seconds()
		err := token.Error()
		if err == nil {
			err = fmt.Errorf("timeout")
		}
		logf("_start_client: MQTT connection failed after %.2f seconds: %v\n", elapsed, err)
		p.mu.Lock()
		p.state = "disconnected"
		p.disconnectCount++
		p.signalLocked()
		p.mu.Unlock()
		p.cleanupClient()
		return
	}
	logf("MQTT client connected successfully after %.2f seconds\n", time.Since(connectStart).Seconds())
}

func (p *PowerStateMQTT) cleanupClient() {
	p.mu.Lock()
	client := p.client
	p.client = nil
	p.mu.Unlock()
	if client != nil && client.IsConnected() {
		client.Disconnect(250)
	}
}

func (p *PowerStateMQTT) onMessage(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	payload := strings.TrimSpace(string(msg.Payload()))
	logf("Received MQTT message: %s %s\n", topic, payload)

	cleanupNeeded := false
	p.mu.Lock()
	if strings.HasSuffix(topic, "/LWT") {
		if payload == "Online" {
			logln("MQTT LWT indicates device is online")
		} else {
			logln("MQTT LWT indicates device is offline")
			cleanupNeeded = true
		}
	} else if strings.HasSuffix(topic, "/POWER") {
		if payload == "ON" || payload == "OFF" {
			logf("MQTT POWER state changed to %s\n", payload)
			p.state = strings.ToLower(payload)
			p.signalLocked()
		}
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	if cleanupNeeded {
		p.mu.Lock()
		if p.state != "loading" {
			p.state = "loading"
			p.loadingCount++
			logln("mqtt_manager: state set to 'loading', count:", p.loadingCount)
			p.signalLocked()
		}
		p.mu.Unlock()
		p.cleanupClient()
	}
}

func (p *PowerStateMQTT) signalLocked() {
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func atoiDefault(s string, fallback int) int {
	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return fallback
	}
	return v
}

func logf(format string, args ...any) {
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000 ")+format, args...)
}

func logln(args ...any) {
	all := append([]any{time.Now().Format("2006-01-02 15:04:05.000")}, args...)
	fmt.Println(all...)
}
