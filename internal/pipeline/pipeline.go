package pipeline

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/pitshifer/volatility/internal/aggregator"
	"github.com/pitshifer/volatility/internal/instruments"
	"github.com/pitshifer/volatility/internal/model"
)

type QuoteSource interface {
	Subscribe(ctx context.Context, symbol string) <-chan model.Quote
}

type Pipeline struct {
	windowSize  time.Duration
	instruments []instruments.Instrument
	quoteSource QuoteSource
}

func New(windowSize time.Duration, instrs []instruments.Instrument, quoteSrc QuoteSource) *Pipeline {
	return &Pipeline{
		windowSize:  windowSize,
		instruments: instrs,
		quoteSource: quoteSrc,
	}
}

func (p *Pipeline) Run(ctx context.Context) {
	wg := &sync.WaitGroup{}
	wg.Add(len(p.instruments))

	for _, instr := range p.instruments {
		go func() {
			defer wg.Done()
			p.runWorker(ctx, instr)
		}()
	}

	wg.Wait()
}

func (p *Pipeline) runWorker(ctx context.Context, instr instruments.Instrument) {
	quoteCh := p.quoteSource.Subscribe(ctx, instr.Symbol)
	aggr := aggregator.NewAggregator(p.windowSize)
	ticker := time.NewTicker(time.Minute)

	go func() {
		var maxVolatility float64
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				volatility := aggr.Volatility()
				roundedVolatility := math.Round(volatility*100) / 100
				if volatility >= instr.Threshold {
					if maxVolatility < volatility {
						maxVolatility = volatility
						// send a message to Kafka
					}
				} else {
					// reset max volatility
					maxVolatility = 0
				}

				slog.Info("volatility", "symbol", instr.Symbol, "volatility", roundedVolatility)
			case q, ok := <-quoteCh:
				if !ok {
					slog.Info("worker stopped", "symbol", instr.Symbol)
					return
				}
				aggr.AddPrice(q.Price, q.TradeTime)
			}
		}
	}()
}
