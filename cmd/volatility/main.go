package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pitshifer/volatility/internal/cfgclient"
	"github.com/pitshifer/volatility/internal/config"
	"github.com/pitshifer/volatility/internal/instruments"
	"github.com/pitshifer/volatility/internal/model"
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
	allInstruments := instrumentManager.GetInstruments()
	if len(allInstruments) == 0 {
		return fmt.Errorf("list instrument is empty")
	}

	// alerting channel for notifier and pipeline
	alertCh := make(chan model.Alert, len(allInstruments)*20)

	// Kafka producer
	kafkaProducer := notifier.NewProducer(cfg.KafkaTopic, cfg.KafkaBrokers, alertCh)
	producerContext, producerCancel := context.WithCancel(context.Background())
	defer producerCancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		kafkaProducer.Run(producerContext)
	}()

	defer func() {
		if err := kafkaProducer.Close(); err != nil {
			slog.Error("closing kafka producer", "error", err)
		}
	}()

	// Pipeline
	pl := pipeline.New(cfg.WindowSize, allInstruments, streamerClient, alertCh)
	pl.Run(ctx)

	select {
	case <-done:
	case <-time.After(cfg.ShutdownTimeout * time.Second):
		producerCancel()
	}

	slog.Info("shutting down...")

	return nil
}
