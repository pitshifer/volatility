package cfgclient

import (
	"context"

	"github.com/pitshifer/volatility/internal/instruments"
)

type DummyClient struct {
}

func NewDummyClient() *DummyClient {
	return &DummyClient{}
}

func (c *DummyClient) Instruments(ctx context.Context) ([]instruments.Instrument, error) {
	return []instruments.Instrument{
		{Symbol: "btcusdt", Threshold: 0.02},
	}, nil
}
