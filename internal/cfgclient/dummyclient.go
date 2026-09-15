package cfgclient

import (
	"context"

	"github.com/pitshifer/volatility/internal/instruments"
)

type DummyClient struct {
	Addr string
}

func NewDummyClient(addr string) *DummyClient {
	return &DummyClient{
		Addr: addr,
	}
}

func (c *DummyClient) Instruments(ctx context.Context) ([]instruments.Instrument, error) {
	return []instruments.Instrument{
		{Symbol: "btcusdt", Threshold: 0.2},
	}, nil
}
