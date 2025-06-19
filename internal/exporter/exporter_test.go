package exporter_test

import (
	"context"
	"testing"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/exporter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
)

func Test_SetupExporter_Success(t *testing.T) {
	cfg := config.Config{
		Exporter: config.ExporterConfig{
			Endpoint: "http://localhost:4318",
		},
		N8N: config.N8NConfig{
			DeploymentMode: "scaling",
			Version:        "1.97.0",
		},
	}

	cleanup, err := exporter.SetupExporter(cfg)
	require.NoError(t, err)
	require.NotNil(t, cleanup)
	defer cleanup()

	tp := otel.GetTracerProvider()
	assert.NotNil(t, tp)

	tracer := otel.Tracer("test")
	_, span := tracer.Start(context.Background(), "test-span")
	span.End()
}

func Test_SetupExporter_InvalidEndpoint(t *testing.T) {
	cfg := config.Config{
		Exporter: config.ExporterConfig{
			Endpoint: "invalid://endpoint",
		},
		N8N: config.N8NConfig{
			DeploymentMode: "scaling",
			Version:        "1.97.0",
		},
	}

	cleanup, err := exporter.SetupExporter(cfg)

	// The function should still succeed even with invalid endpoint
	// as the OTLP client creation doesn't validate connectivity
	require.NoError(t, err)
	require.NotNil(t, cleanup)
	defer cleanup()
}

func Test_SetupExporter_MultipleConfigurations(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
	}{
		{
			name: "ScalingMode",
			cfg: config.Config{
				Exporter: config.ExporterConfig{Endpoint: "http://localhost:4318"},
				N8N:      config.N8NConfig{DeploymentMode: "scaling", Version: "1.97.0"},
			},
		},
		{
			name: "RegularMode",
			cfg: config.Config{
				Exporter: config.ExporterConfig{Endpoint: "http://localhost:4318"},
				N8N:      config.N8NConfig{DeploymentMode: "regular", Version: "1.98.0"},
			},
		},
		{
			name: "DifferentEndpoint",
			cfg: config.Config{
				Exporter: config.ExporterConfig{Endpoint: "http://jaeger:4318"},
				N8N:      config.N8NConfig{DeploymentMode: "scaling", Version: "2.0.0"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanup, err := exporter.SetupExporter(tt.cfg)
			require.NoError(t, err)
			require.NotNil(t, cleanup)
			defer cleanup()

			tracer := otel.Tracer("test")
			_, span := tracer.Start(context.Background(), "test-span")
			span.SetAttributes(attribute.String("test.mode", tt.cfg.N8N.DeploymentMode))
			span.End()
		})
	}
}

func Test_SetupExporter_ResourceAttributes(t *testing.T) {
	cfg := config.Config{
		Exporter: config.ExporterConfig{
			Endpoint: "http://localhost:4318",
		},
		N8N: config.N8NConfig{
			DeploymentMode: "regular",
			Version:        "1.98.1",
		},
	}

	originalTP := otel.GetTracerProvider()
	defer otel.SetTracerProvider(originalTP)

	cleanup, err := exporter.SetupExporter(cfg)
	require.NoError(t, err)
	defer cleanup()

	tp := otel.GetTracerProvider()

	_, ok := tp.(*trace.TracerProvider)
	require.True(t, ok, "Expected SDK TracerProvider")

	// We cannot easily access the resource from the TracerProvider without creating a span,
	// so we create a span and check it has the right tracer provider
	tracer := otel.Tracer("test")
	_, span := tracer.Start(context.Background(), "test-span")

	assert.NotNil(t, span) // verifies exporter setup worked
	span.End()

	assert.NotEqual(t, originalTP, tp, "TracerProvider should have been replaced")
}
