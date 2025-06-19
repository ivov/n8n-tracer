package config

import (
	"context"
	"fmt"
	"time"

	"github.com/sethvargo/go-envconfig"
)

type Config struct {
	N8N             N8NConfig
	LogfileIngestor LogfileIngestorConfig
	HTTPIngestor    HTTPIngestorConfig
	Exporter        ExporterConfig
	Health          HealthConfig
}

type N8NConfig struct {
	DeploymentMode string `env:"N8N_DEPLOYMENT_MODE,required"`

	Version string `env:"N8N_VERSION,default=1.97.0"`

	WorkflowStartOffset time.Duration `env:"N8N_WORKFLOW_START_OFFSET,default=50ms"`
}

type LogfileIngestorConfig struct {
	WatchFilePath string `env:"WATCH_FILE_PATH"`

	StateFilePath string `env:"STATE_FILE_PATH,default=n8n-tracer.state.json"`

	DebounceDuration time.Duration `env:"DEBOUNCE_DURATION,default=1s"`
}

type HTTPIngestorConfig struct {
	Port string `env:"HTTP_INGEST_PORT,default=8889"`
}

type ExporterConfig struct {
	Endpoint string `env:"OTEL_EXPORTER_OTLP_ENDPOINT,default=http://localhost:4318"`
}

type HealthConfig struct {
	Port string `env:"HEALTH_PORT,default=8888"`

	StaleSpanThreshold time.Duration `env:"STALE_SPAN_THRESHOLD,default=24h"`

	SpanGCInterval time.Duration `env:"SPAN_GC_INTERVAL,default=1h"`
}

func LoadConfig(ctx context.Context, lookuper envconfig.Lookuper) (*Config, error) {
	var c Config
	if err := envconfig.ProcessWith(ctx, &envconfig.Config{
		Target:   &c,
		Lookuper: lookuper,
	}); err != nil {
		return nil, err
	}

	if err := validateConfig(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

func validateConfig(c *Config) error {
	switch c.N8N.DeploymentMode {
	case "regular":
		if c.LogfileIngestor.WatchFilePath == "" {
			return fmt.Errorf("WATCH_FILE_PATH is required when N8N_DEPLOYMENT_MODE is 'regular'")
		}
	case "scaling":
		// No additional validation needed for scaling mode
	default:
		return fmt.Errorf("N8N_DEPLOYMENT_MODE must be either 'regular' or 'scaling', got: %s", c.N8N.DeploymentMode)
	}
	return nil
}
