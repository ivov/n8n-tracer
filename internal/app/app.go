package app

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
	"github.com/ivov/n8n-tracer/internal/exporter"
	"github.com/ivov/n8n-tracer/internal/health"
	"github.com/ivov/n8n-tracer/internal/ingestion"
	httpingestion "github.com/ivov/n8n-tracer/internal/ingestion/http"
	"github.com/ivov/n8n-tracer/internal/ingestion/logfile"
)

type App struct {
	cfg             config.Config
	tracer          *core.Tracer
	ingester        ingestion.Ingester
	exporterCleanup func()
	metrics         *Metrics
}

type Metrics struct {
	eventsProcessed      atomic.Int64
	lastEventProcessedAt atomic.Value // time.Time
}

func (a *App) GetMetrics() (int64, time.Time, int) {
	lastAt, _ := a.metrics.lastEventProcessedAt.Load().(time.Time)
	return a.metrics.eventsProcessed.Load(), lastAt, a.tracer.ExecutionStatesInMemory()
}

func New(cfg config.Config, tracer *core.Tracer) (*App, error) {
	exporterCleanup, err := exporter.SetupExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to set up exporter: %w", err)
	}

	var ingester ingestion.Ingester

	switch cfg.N8N.DeploymentMode {
	case "regular":
		stateManager, err := logfile.NewStateManager(cfg.LogfileIngestor.StateFilePath)
		if err != nil {
			exporterCleanup()
			return nil, fmt.Errorf("failed to initialize state manager: %w", err)
		}

		watcher, err := logfile.NewLogfileWatcher(cfg, stateManager)
		if err != nil {
			exporterCleanup()
			return nil, fmt.Errorf("failed to initialize logfile watcher: %w", err)
		}
		ingester = watcher

	case "scaling":
		server := httpingestion.NewHTTPIngestorServer(cfg.HTTPIngestor)
		ingester = server

	default:
		exporterCleanup()
		return nil, fmt.Errorf("invalid deployment mode: %s (must be 'regular' or 'scaling')", cfg.N8N.DeploymentMode)
	}

	return &App{
		cfg:             cfg,
		tracer:          tracer,
		ingester:        ingester,
		exporterCleanup: exporterCleanup,
		metrics:         &Metrics{},
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	defer a.exporterCleanup()

	// This `WaitGroup` coordinates the graceful shutdown of long-running background
	// services that are not directly tied to the main event processing loop.
	//
	// Shutdown sequence:
	//
	// 1. A shutdown signal (e.g., Ctrl+C) cancels the main `context`.
	//
	// 2. The main loop's `select` statement detects `<-ctx.Done()` and calls `a.ingester.Stop()`,
	//    which in turn closes the `eventChan`. Meanwhile, the other goroutines have also
	//    detected `<-ctx.Done()` and have begun their own shutdown procedures.
	//
	// 3. The main loop then detects the closed `eventChan`, meaning that the ingester has finished.
	//
	// 4. The main loop then calls `wg.Wait()`, so we wait for the other goroutines (GC and health server)
	//    to call `wg.Done()`.
	//
	// 5. Once the `WaitGroup` counter reaches zero, `wg.Wait()` unblocks. This allows `app.Run` to return,
	// ensuring all background tasks have terminated before the program exits.

	// Shutdown sequence
	//
	// t=0ms:  SIGINT/SIGTERM received
	// t=1ms:  Signal handler catches signal -> Cancels context -> `ctx.Done()` in all goroutines
	// t=2ms:  │
	//         ├── Main loop: `a.ingester.Stop()`
	//         ├── Health server: `srv.Shutdown()`
	//         ├── GC: exits
	//         └── Ingester: stops watching or listening
	// t=5ms:  Ingester closes `eventCh` and `errorCh`
	// t=6ms:  Main loop detects closed channels -> Calls `wg.Wait()` to wait for all goroutines to finish
	// t=10ms: All goroutines finish -> `wg.Wait()` unblocks
	//
	// t=11ms: App.Run() returns → exporterCleanup() -> main() exits → "Completed graceful shutdown"
	var wg sync.WaitGroup

	healthCheckServer := health.NewHealthCheckServer(a.cfg.Health.Port, a)

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting health check server on %s", healthCheckServer.Addr)

		if err := healthCheckServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if opErr, ok := err.(*net.OpError); ok && opErr.Op == "listen" {
				log.Printf("Health check server failed to start: Port %s is already in use", healthCheckServer.Addr)
			} else {
				log.Printf("Health check server failed to start: %v", err)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := healthCheckServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Health server shutdown error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		a.tracer.StartGC(ctx, a.cfg.Health.StaleSpanThreshold, a.cfg.Health.SpanGCInterval)
	}()

	eventChan, errorChan := a.ingester.Start(ctx)
	shutdownStarted := false

	for {
		select {
		case event, ok := <-eventChan:
			if !ok {
				wg.Wait()
				return nil // Event channel was closed, all serves were shut down
			}
			if err := a.tracer.ProcessEvent(event); err != nil {
				log.Printf("Failed to process event: %v", err)
			} else {
				a.metrics.eventsProcessed.Add(1)
				a.metrics.lastEventProcessedAt.Store(time.Now())
			}
		case err, ok := <-errorChan:
			if !ok {
				log.Println("Error channel closed. Exiting...")
				return nil
			}
			log.Printf("Ingestion error: %v", err)
		case <-ctx.Done():
			if !shutdownStarted {
				log.Println("Shutdown signal received")
				a.ingester.Stop()
				shutdownStarted = true
			}
		}
	}
}
