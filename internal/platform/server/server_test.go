package server_test

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/server"
)

func TestRunShutsDownOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.DiscardHandler)

	srv := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
	shutdownHookCalled := make(chan struct{})

	done := make(chan error, 1)
	go func() {
		done <- server.Run(ctx, logger, time.Second, func() { close(shutdownHookCalled) }, srv)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
	select {
	case <-shutdownHookCalled:
	default:
		t.Error("beforeShutdown hook was not called")
	}
}

func TestRunFailsFastOnInvalidAddress(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	good := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: time.Second}
	bad := &http.Server{Addr: "127.0.0.1:not-a-port", ReadHeaderTimeout: time.Second}

	if err := server.Run(context.Background(), logger, time.Second, nil, good, bad); err == nil {
		t.Error("Run() error = nil, want listen error")
	}
}
