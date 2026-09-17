package streamer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	apiv1 "github.com/pitshifer/volatility/internal/gen/api/v1"
	"github.com/pitshifer/volatility/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	baseRetryDelay = time.Second
	maxRetryDelay  = 30 * time.Second
	jitterFactor   = 0.2
)

type Client struct {
	conn *grpc.ClientConn
	api  apiv1.StreamerServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("streamer: dial %s: %w", addr, err)
	}

	return &Client{
		conn: conn,
		api:  apiv1.NewStreamerServiceClient(conn),
	}, nil
}

func (c *Client) Subscribe(ctx context.Context, symbol string) <-chan model.Quote {
	quoteCh := make(chan model.Quote, 30)

	go func() {
		defer close(quoteCh)

		delay := baseRetryDelay
		for {
			stream, err := c.api.Quote(ctx, &apiv1.QuoteRequest{Symbol: symbol})
			if err == nil {
				var resp *apiv1.QuoteResponse
				for {
					resp, err = stream.Recv()
					if err != nil {
						break
					}
					delay = baseRetryDelay

					select {
					case <-ctx.Done():
						return
					case quoteCh <- model.Quote{
						Symbol:    resp.Symbol,
						Price:     resp.Price,
						TradeTime: time.Unix(resp.TradeTime, 0),
					}:
					}

				}
			}

			if ctx.Err() != nil {
				return
			}
			if status.Code(err) == codes.NotFound {
				slog.Warn("symbol is not found", "symbol", symbol)
				return
			}
			if errors.Is(err, io.EOF) {
				slog.Info("streamer service closed connection", "symbol", symbol)
			} else {
				slog.Warn("streamer error", "error", err, "symbol", symbol)
			}

			wait := withJitter(delay)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			delay = min(delay*2, maxRetryDelay)
		}
	}()

	return quoteCh
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func withJitter(delay time.Duration) time.Duration {
	delta := float64(delay) * jitterFactor
	return time.Duration(float64(delay) - delta + rand.Float64()*2*delta)
}
