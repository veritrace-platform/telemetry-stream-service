// Package server runs HTTP servers until the context is cancelled, then shuts them down gracefully.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Run starts every server and blocks until ctx is cancelled or a server fails. It then calls
// beforeShutdown (for example to fail readiness) and shuts all servers down within timeout.
func Run(ctx context.Context, logger *slog.Logger, timeout time.Duration, beforeShutdown func(), servers ...*http.Server) error {
	var lc net.ListenConfig
	listeners := make([]net.Listener, 0, len(servers))
	for _, srv := range servers {
		ln, err := lc.Listen(ctx, "tcp", srv.Addr)
		if err != nil {
			for _, open := range listeners {
				_ = open.Close()
			}
			return fmt.Errorf("listen on %s: %w", srv.Addr, err)
		}
		listeners = append(listeners, ln)
	}

	serveErr := make(chan error, len(servers))
	for i, srv := range servers {
		logger.InfoContext(ctx, "server listening", slog.String("addr", listeners[i].Addr().String()))
		go func() {
			if err := srv.Serve(listeners[i]); err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("serve %s: %w", srv.Addr, err)
			}
		}()
	}

	var runErr error
	select {
	case <-ctx.Done():
		logger.InfoContext(ctx, "shutdown signal received")
	case runErr = <-serveErr:
		logger.ErrorContext(ctx, "server failed", slog.Any("error", runErr))
	}

	if beforeShutdown != nil {
		beforeShutdown()
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	var shutdownErrs []error
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("shutdown %s: %w", srv.Addr, err))
		}
	}
	return errors.Join(runErr, errors.Join(shutdownErrs...))
}
