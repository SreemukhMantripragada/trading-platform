package shared

import "time"

// Tick mirrors the Python ingress schema.
type Tick struct {
	Symbol  string  `json:"symbol"`
	EventTS int64   `json:"event_ts"` // nanoseconds epoch
	LTP     float64 `json:"ltp"`
	Vol     int64   `json:"vol"`
}

func (t Tick) EventTime() time.Time {
	return time.Unix(0, t.EventTS)
}

// Bar1s represents the 1-second bar payload.
type Bar1s struct {
	Symbol   string  `json:"symbol"`
	TF       string  `json:"tf"`
	TS       int64   `json:"ts"` // seconds epoch
	O        float64 `json:"o"`
	H        float64 `json:"h"`
	L        float64 `json:"l"`
	C        float64 `json:"c"`
	Vol      int64   `json:"vol"`
	NTrades  int64   `json:"n_trades"`
}

// BarTF represents aggregated timeframes (3m/5m/etc.).
type BarTF struct {
	Symbol  string  `json:"symbol"`
	TS      int64   `json:"ts"` // bucket start seconds
	O       float64 `json:"o"`
	H       float64 `json:"h"`
	L       float64 `json:"l"`
	C       float64 `json:"c"`
	Vol     int64   `json:"vol"`
	NTrades int64   `json:"n_trades"`
}

func (b BarTF) WindowEnd(tfSeconds int64) int64 {
	return b.TS + tfSeconds
}
