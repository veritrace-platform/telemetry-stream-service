// Package buildinfo exposes build metadata injected at link time.
package buildinfo

// Version is set with -ldflags "-X github.com/veritrace-platform/telemetry-stream-service/internal/platform/buildinfo.Version=<version>".
var Version = "dev"

// ServiceName identifies this service in logs, metrics, and events.
const ServiceName = "telemetry-stream-service"
