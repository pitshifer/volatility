package model

import "time"

type Quote struct {
	Symbol    string
	Price     float64
	TradeTime time.Time
}
