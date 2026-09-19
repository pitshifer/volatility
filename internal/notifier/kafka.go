package notifier

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/pitshifer/volatility/internal/model"
	"github.com/segmentio/kafka-go"
)

const schemaVersion uint = 1

type message struct {
	Version uint `json:"version"`
	model.Alert
}

type Producer struct {
	writer  *kafka.Writer
	alertCh <-chan model.Alert
}

func NewProducer(topic string, brokers []string, alertCh <-chan model.Alert) *Producer {
	return &Producer{
		alertCh: alertCh,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			RequiredAcks:           kafka.RequireOne,
			AllowAutoTopicCreation: true,
			Async:                  false,
		},
	}
}

func (p *Producer) Run(ctx context.Context) {
	for alert := range p.alertCh {
		if err := p.Notify(ctx, alert); err != nil {
			slog.Error("failed to send an alert to kafka", "error", err, "symbol", alert.Symbol)
		}
	}
}

func (p *Producer) Notify(ctx context.Context, alert model.Alert) error {
	msg := message{
		Version: schemaVersion,
		Alert:   alert,
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(alert.Symbol),
		Value: payload,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
