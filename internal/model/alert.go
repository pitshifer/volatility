package model

import "time"

type Alert struct {
	Symbol     string    `json:"symbol"`
	Volatility float64   `json:"volatility"`
	Threshold  float64   `json:"threshold"`
	Timestamp  time.Time `json:"timestamp"`
}
