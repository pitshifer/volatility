package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pitshifer/volatility/internal/cfgclient"
	"github.com/pitshifer/volatility/internal/config"
	"github.com/pitshifer/volatility/internal/instruments"
	"github.com/pitshifer/volatility/internal/notifier"
	"github.com/pitshifer/volatility/internal/pipeline"
	"github.com/pitshifer/volatility/internal/streamer"
)

func main() {
	if err := run(); err != nil {
		slog.Error("service failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Config
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config failed: %w", err)
	}

	// Streamer
	streamerClient, err := streamer.NewClient(cfg.StreamerAddr)
	if err != nil {
		return fmt.Errorf("streamer: %w", err)
	}
	defer streamerClient.Close()

	// Instrument manager
	instrumentManager := instruments.NewManager(cfgclient.NewDummyClient())
	if err = instrumentManager.Load(ctx); err != nil {
		return fmt.Errorf("load instruments: %w", err)
	}
	if len(instrumentManager.GetInstruments()) == 0 {
		return fmt.Errorf("list instrument is empty")
	}

	// Kafka producer
	kafkaProducer := notifier.NewProducer(cfg.KafkaTopic, cfg.KafkaBrokers)
	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			slog.Error("closing kafka producer", "error", err)
		}
	}()

	// Pipeline
	pl := pipeline.New(cfg.WindowSize, instrumentManager.GetInstruments(), streamerClient, kafkaProducer)
	pl.Run(ctx)

	slog.Info("shutting down...")

	return nil
}
