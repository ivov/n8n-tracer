package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/ivov/n8n-tracer/internal/app"
	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
	"github.com/sethvargo/go-envconfig"
)

func main() {
	log.Printf("n8n-tracer is starting... Press Ctrl+C to exit")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadConfig(ctx, envconfig.OsLookuper())
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	tracer := core.NewTracer(cfg.N8N)

	application, err := app.New(*cfg, tracer)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	if err := application.Run(ctx); err != nil {
		log.Fatalf("Application failed to start processing: %v", err)
	}

	log.Println("Completed graceful shutdown")
}
