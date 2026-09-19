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
	alertCh     chan<- model.Alert
}

func New(windowSize time.Duration, instrs []instruments.Instrument, quoteSrc QuoteSource, alertCh chan<- model.Alert) *Pipeline {
	return &Pipeline{
		windowSize:  windowSize,
		instruments: instrs,
		quoteSource: quoteSrc,
		alertCh:     alertCh,
	}
}

func (p *Pipeline) Run(ctx context.Context) {
	defer close(p.alertCh)

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
					alert := model.Alert{
						Symbol:     instr.Symbol,
						Volatility: roundedVolatility,
						Threshold:  instr.Threshold,
						Timestamp:  time.Now().UTC(),
					}
					select {
					case p.alertCh <- alert:
						maxVolatility = volatility
					default:
						slog.Warn("alert dropped, channel full", "symbol", instr.Symbol)
					}
				}
			} else {
				// reset max volatility
				maxVolatility = 0
			}

			slog.Info("volatility", "symbol", instr.Symbol, "volatility", roundedVolatility, "Threshold", instr.Threshold)
		case q, ok := <-quoteCh:
			if !ok {
				slog.Info("worker stopped", "symbol", instr.Symbol)
				return
			}
			aggr.AddPrice(q.Price, q.TradeTime)
		}
	}
}
