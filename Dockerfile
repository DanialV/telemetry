FROM golang:1.26-alpine AS builder

WORKDIR /src
ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingestor ./cmd/ingestor && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/processor ./cmd/processor && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/query-api ./cmd/query-api && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/simulator ./scripts/simulator && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/smoketest ./scripts/smoketest && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/loadtest ./scripts/loadtest

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=builder /out/ /usr/local/bin/
