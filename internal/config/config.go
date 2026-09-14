package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	StreamerAddr  string
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaTopicDLQ string
	WindowSize    time.Duration
	LogLevel      slog.Level
}

func Load() (*Config, error) {
	logLevel, err := getEnv("LOG_LEVEL", func(s string) (slog.Level, error) {
		var level slog.Level
		err := level.UnmarshalText([]byte(s))
		return level, err
	})
	if err != nil {
		return nil, err
	}

	kafkaBrokers, err := getEnv("KAFKA_BROKERS", func(s string) ([]string, error) {
		return strings.Split(s, ","), nil
	})
	if err != nil {
		return nil, err
	}

	kafkaTopic, err := getEnv("KAFKA_TOPIC", identity)
	if err != nil {
		return nil, err
	}

	kafkaTopicDLQ, err := getEnv("KAFKA_TOPIC_DLQ", identity)
	if err != nil {
		return nil, err
	}

	windowSize, err := getEnv("WINDOW_SIZE", func(s string) (time.Duration, error) {
		size, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		return time.Duration(size) * time.Second, nil
	})
	if err != nil {
		return nil, err
	}

	streamerAddr, err := getEnv("STREAMER_ADDR", identity)
	if err != nil {
		return nil, err
	}

	return &Config{
		LogLevel:      logLevel,
		KafkaBrokers:  kafkaBrokers,
		KafkaTopic:    kafkaTopic,
		KafkaTopicDLQ: kafkaTopicDLQ,
		WindowSize:    windowSize,
		StreamerAddr:  streamerAddr,
	}, nil
}

func getEnv[T any](key string, parse func(string) (T, error)) (T, error) {
	var zero T

	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return zero, fmt.Errorf("%s is not set", key)
	}

	v, err := parse(raw)
	if err != nil {
		return zero, fmt.Errorf("failed to parse %s: %v", key, err)
	}

	return v, nil
}

func identity(s string) (string, error) {
	return s, nil
}
