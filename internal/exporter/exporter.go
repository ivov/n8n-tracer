package exporter

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

type loggingExporter struct {
	exporter trace.SpanExporter
}

func (l *loggingExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	err := l.exporter.ExportSpans(ctx, spans)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") ||
			strings.Contains(err.Error(), "no such host") {
			log.Printf("Cannot reach OTLP endpoint")
			log.Printf("Will buffer traces until OTLP endpoint becomes available")
		} else {
			log.Printf("Failed to export spans: %v", err)
		}
		return err
	}

	log.Printf("Exported %d spans", len(spans))

	return nil
}

func (l *loggingExporter) Shutdown(ctx context.Context) error {
	return l.exporter.Shutdown(ctx)
}

func SetupExporter(cfg config.Config) (func(), error) {
	log.Printf("Configured for OTLP endpoint: %s", cfg.Exporter.Endpoint)

	if err := testConnection(cfg.Exporter.Endpoint); err != nil {
		log.Printf("Cannot reach OTLP endpoint")
		log.Printf("Will buffer traces until OTLP endpoint becomes available")
	} else {
		log.Printf("Connected to OTLP endpoint")
	}

	baseExporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpointURL(cfg.Exporter.Endpoint),
		),
	)
	if err != nil {
		return nil, err
	}

	exporter := &loggingExporter{exporter: baseExporter}

	resource, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("n8n"),
			semconv.ServiceVersion("n8n@"+cfg.N8N.Version),
			attribute.String("service.mode", cfg.N8N.DeploymentMode),
			semconv.TelemetrySDKName("opentelemetry"),
			semconv.TelemetrySDKLanguageGo,
			semconv.TelemetrySDKVersion("otel@"+otel.Version()),
			attribute.String("telemetry.instrumentation.name", "n8n-tracer"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource),
	)

	otel.SetTracerProvider(tp)

	return func() {
		_ = tp.Shutdown(context.Background())
	}, nil
}

func testConnection(endpoint string) error {
	client := &http.Client{Timeout: 3 * time.Second}

	// Most OTLP endpoints will respond with 405 Method Not Allowed for GET,
	// but that confirms the endpoint is reachable
	resp, err := client.Get(endpoint + "/v1/traces")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
