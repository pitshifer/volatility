package aggregator

import "time"

type Aggregator struct {
	window time.Duration
	prices []pricePoint
}

type pricePoint struct {
	price float64
	at    time.Time
}

func NewAggregator(window time.Duration) *Aggregator {
	return &Aggregator{
		window: window,
	}
}

func (a *Aggregator) AddPrice(price float64, at time.Time) {
	a.prices = append(a.prices, pricePoint{price: price, at: at})
	a.prune(at)
}

func (a *Aggregator) prune(now time.Time) {
	cutoff := now.Add(-a.window)
	i := 0
	for i < len(a.prices) && a.prices[i].at.Before(cutoff) {
		i++
	}
	a.prices = a.prices[i:]
}

func (a *Aggregator) Volatility() float64 {
	if len(a.prices) < 2 {
		return 0
	}

	low, hi := a.prices[0].price, a.prices[0].price
	for _, p := range a.prices {
		if p.price < low {
			low = p.price
		}
		if p.price > hi {
			hi = p.price
		}
	}
	return (hi - low) / low * 100
}
