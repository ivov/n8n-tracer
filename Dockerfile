# syntax=docker/dockerfile:1

FROM golang:1.24.4-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -ldflags="-w -s" -o n8n-tracer cmd/main.go

ARG TRACER_VERSION=dev
ARG TRACER_COMMIT=unknown
LABEL org.opencontainers.image.version=${TRACER_VERSION}
LABEL org.opencontainers.image.revision=${TRACER_COMMIT}

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/n8n-tracer .
RUN adduser -D -s /bin/sh tracer
RUN chown -R tracer:tracer /app
USER tracer
EXPOSE 8888
CMD ["./n8n-tracer"]
