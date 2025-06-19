// internal/config/config_test.go
package config

import (
	"context"
	"testing"
	"time"

	"github.com/sethvargo/go-envconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_LoadConfig_DefaultValues_RegularMode(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "regular",
		"WATCH_FILE_PATH":     "/test/path",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)
	require.NoError(t, err)

	assert.Equal(t, "/test/path", cfg.LogfileIngestor.WatchFilePath)
	assert.Equal(t, "n8n-tracer.state.json", cfg.LogfileIngestor.StateFilePath)
	assert.Equal(t, 1*time.Second, cfg.LogfileIngestor.DebounceDuration)
	assert.Equal(t, "regular", cfg.N8N.DeploymentMode)
	assert.Equal(t, "1.97.0", cfg.N8N.Version)
	assert.Equal(t, "http://localhost:4318", cfg.Exporter.Endpoint)
	assert.Equal(t, "8888", cfg.Health.Port)
	assert.Equal(t, 24*time.Hour, cfg.Health.StaleSpanThreshold)
	assert.Equal(t, 1*time.Hour, cfg.Health.SpanGCInterval)
	assert.Equal(t, "8889", cfg.HTTPIngestor.Port)
}

func Test_LoadConfig_DefaultValues_ScalingMode(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "scaling",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)
	require.NoError(t, err)

	assert.Equal(t, "scaling", cfg.N8N.DeploymentMode)
	assert.Equal(t, "8889", cfg.HTTPIngestor.Port)
	assert.Equal(t, "", cfg.LogfileIngestor.WatchFilePath)
}

func Test_LoadConfig_CustomValues(t *testing.T) {
	envVars := map[string]string{
		"WATCH_FILE_PATH":             "/custom/path/logfile.log",
		"STATE_FILE_PATH":             "/custom/state.json",
		"DEBOUNCE_DURATION":           "500ms",
		"N8N_DEPLOYMENT_MODE":         "regular",
		"N8N_VERSION":                 "2.0.0",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://custom:4319",
		"HEALTH_PORT":                 "9090",
		"STALE_SPAN_THRESHOLD":        "1h",
		"SPAN_GC_INTERVAL":            "15m",
		"HTTP_INGEST_PORT":            "9999",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)
	require.NoError(t, err)

	assert.Equal(t, "/custom/path/logfile.log", cfg.LogfileIngestor.WatchFilePath)
	assert.Equal(t, "/custom/state.json", cfg.LogfileIngestor.StateFilePath)
	assert.Equal(t, 500*time.Millisecond, cfg.LogfileIngestor.DebounceDuration)
	assert.Equal(t, "regular", cfg.N8N.DeploymentMode)
	assert.Equal(t, "2.0.0", cfg.N8N.Version)
	assert.Equal(t, "http://custom:4319", cfg.Exporter.Endpoint)
	assert.Equal(t, "9090", cfg.Health.Port)
	assert.Equal(t, 1*time.Hour, cfg.Health.StaleSpanThreshold)
	assert.Equal(t, 15*time.Minute, cfg.Health.SpanGCInterval)
	assert.Equal(t, "9999", cfg.HTTPIngestor.Port)
}

func Test_LoadConfig_RequiredFieldMissing_DeploymentMode(t *testing.T) {
	envVars := map[string]string{
		"WATCH_FILE_PATH": "/test/path",
		// N8N_DEPLOYMENT_MODE is missing
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "missing required value: N8N_DEPLOYMENT_MODE")
}

func Test_LoadConfig_RegularMode_RequiresWatchFilePath(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "regular",
		// WATCH_FILE_PATH is missing
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "WATCH_FILE_PATH is required when N8N_DEPLOYMENT_MODE is 'regular'")
}

func Test_LoadConfig_ScalingMode_Success(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "scaling",
		"HTTP_INGEST_PORT":    "9999",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)
	require.NoError(t, err)

	assert.Equal(t, "scaling", cfg.N8N.DeploymentMode)
	assert.Equal(t, "9999", cfg.HTTPIngestor.Port)
	assert.Equal(t, "", cfg.LogfileIngestor.WatchFilePath)
}

func Test_LoadConfig_InvalidDeploymentMode(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "invalid",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "N8N_DEPLOYMENT_MODE must be either 'regular' or 'scaling'")
}

func Test_LoadConfig_InvalidDuration(t *testing.T) {
	envVars := map[string]string{
		"N8N_DEPLOYMENT_MODE": "regular",
		"WATCH_FILE_PATH":     "/test/path",
		"DEBOUNCE_DURATION":   "not-a-duration",
	}
	lookuper := envconfig.MapLookuper(envVars)

	cfg, err := LoadConfig(context.Background(), lookuper)

	require.Error(t, err)
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "time: invalid duration")
}
