FROM golang:1.27.1-alpine3.24 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/veritrace-platform/telemetry-stream-service/internal/platform/buildinfo.Version=${VERSION}" \
      -o /out/telemetry-stream-service ./cmd/telemetry-stream-service

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/telemetry-stream-service /app/telemetry-stream-service
USER nonroot:nonroot
EXPOSE 8090 8091
ENTRYPOINT ["/app/telemetry-stream-service"]
CMD ["serve"]
