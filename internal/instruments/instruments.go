package instruments

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"
)

const (
	baseRetryDelay = time.Second
	maxRetryDelay  = 30 * time.Second
	jitterFactor   = 0.2
)

type InstrumentSource interface {
	Instruments(ctx context.Context) ([]Instrument, error)
}

type Instrument struct {
	Symbol    string
	Threshold float64
}

type Manager struct {
	source      InstrumentSource
	instruments atomic.Pointer[[]Instrument]
}

func NewManager(source InstrumentSource) *Manager {
	return &Manager{
		source: source,
	}
}

func (m *Manager) Load(ctx context.Context) error {
	instruments, err := m.source.Instruments(ctx)
	if err != nil {
		return fmt.Errorf("fetch instruments: %w", err)
	}
	m.instruments.Store(&instruments)

	return nil
}

func (m *Manager) Run(ctx context.Context, reload <-chan struct{}) error {
	if err := m.loadUntilSuccess(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reload:
			if err := m.loadUntilSuccess(ctx); err != nil {
				return err
			}
		}
	}
}

func (m *Manager) loadUntilSuccess(ctx context.Context) error {
	delay := baseRetryDelay

	for attempt := 1; ; attempt++ {
		if err := m.Load(ctx); err == nil {
			slog.Info("instruments loaded")
			return nil
		} else {
			slog.Warn("failed to load instruments, retrying", "error", err, "attempt", attempt)

			wait := withJitter(delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		delay = min(delay*2, maxRetryDelay)
	}
}

func (m *Manager) GetInstruments() []Instrument {
	p := m.instruments.Load()
	if p == nil {
		return nil
	}

	return slices.Clone(*p)
}

func withJitter(delay time.Duration) time.Duration {
	delta := float64(delay) * jitterFactor
	return time.Duration(float64(delay) - delta + rand.Float64()*2*delta)
}
