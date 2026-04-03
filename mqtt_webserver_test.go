package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type blockingPowerState struct {
	mu          sync.Mutex
	startedCh   chan struct{}
	returnedCh  chan struct{}
	lastFrom    string
	lastTimeout time.Duration
	startOnce   sync.Once
	returnOnce  sync.Once
}

func newBlockingPowerState() *blockingPowerState {
	return &blockingPowerState{
		startedCh:  make(chan struct{}),
		returnedCh: make(chan struct{}),
	}
}

func (b *blockingPowerState) WaitForChange(ctx context.Context, fromState string, timeout time.Duration) string {
	b.mu.Lock()
	b.lastFrom = fromState
	b.lastTimeout = timeout
	b.mu.Unlock()

	b.startOnce.Do(func() { close(b.startedCh) })
	<-ctx.Done()
	b.returnOnce.Do(func() { close(b.returnedCh) })
	return fromState
}

func (b *blockingPowerState) RequestStateChange(newState string) {}

func (b *blockingPowerState) Close() {}

func TestStreamEventsReturnsOnClientDisconnect(t *testing.T) {
	mock := newBlockingPowerState()

	sharedState.mu.Lock()
	origPower := sharedState.powerState
	origRefs := sharedState.activeStreamRefs
	sharedState.powerState = mock
	sharedState.activeStreamRefs = 1
	sharedState.mu.Unlock()
	defer func() {
		sharedState.mu.Lock()
		sharedState.powerState = origPower
		sharedState.activeStreamRefs = origRefs
		sharedState.mu.Unlock()
	}()

	req := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		streamEvents(w, req)
		close(done)
	}()

	select {
	case <-mock.startedCh:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for WaitForChange to start")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("streamEvents did not return quickly after client disconnect")
	}

	select {
	case <-mock.returnedCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WaitForChange did not return after context cancellation")
	}

	mock.mu.Lock()
	gotFrom := mock.lastFrom
	gotTimeout := mock.lastTimeout
	mock.mu.Unlock()

	if gotFrom != initialState {
		t.Fatalf("expected fromState %q, got %q", initialState, gotFrom)
	}
	if gotTimeout != 20*time.Second {
		t.Fatalf("expected heartbeat timeout %v, got %v", 20*time.Second, gotTimeout)
	}
}
