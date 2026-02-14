package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"trading-platform/go/pkg/shared"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

// Config for bar builder.
type Config struct {
	Kafka    shared.KafkaConfig
	PG       shared.PostgresConfig
	Metrics  shared.MetricsConfig
	Grace    shared.GraceConfig
	InTopic  string `envconfig:"IN_TOPIC" default:"ticks"`
	OutTopic string `envconfig:"OUT_TOPIC" default:"bars.1s"`
}

// BarState holds mutable bar data per symbol.
type BarState struct {
	Sec     int64
	O, H, L, C float64
	Vol     int64
	NTrades int64
}

func (b *BarState) Update(px float64, vol int64) {
	if b.NTrades == 0 {
		b.O, b.H, b.L, b.C = px, px, px, px
		b.Vol = vol
		b.NTrades = 1
		return
	}
	if px > b.H {
		b.H = px
	}
	if px < b.L {
		b.L = px
	}
	b.C = px
	b.Vol += vol
	b.NTrades++
}

// Metrics bundle.
type metrics struct {
	ticks    prometheus.Counter
	barsOut  prometheus.Counter
	openBars prometheus.Gauge
	flushDur prometheus.Histogram
	barLat   prometheus.Histogram
}

func newMetrics() metrics {
	return metrics{
		ticks:    shared.NewCounter(prometheus.CounterOpts{Name: "bars1s_ticks_total", Help: "Ticks processed"}),
		barsOut:  shared.NewCounter(prometheus.CounterOpts{Name: "bars1s_published_total", Help: "Bars published"}),
		openBars: shared.NewGauge(prometheus.GaugeOpts{Name: "bars1s_open_symbols", Help: "Open bar windows"}),
		flushDur: shared.NewHist(prometheus.HistogramOpts{Name: "bars1s_flush_seconds", Help: "Flush duration", Buckets: []float64{0.001, 0.005, 0.01, 0.02, 0.05, 0.1}}),
		barLat:   shared.NewHist(prometheus.HistogramOpts{Name: "bars1s_publish_latency_seconds", Help: "Latency close->publish", Buckets: []float64{0.05, 0.1, 0.2, 0.5, 1, 2}}),
	}
}

// SQL upsert same as Python.
const upsertSQL = `
INSERT INTO bars_1s(symbol, ts, o, h, l, c, vol, n_trades)
VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT(symbol, ts) DO UPDATE
SET o=EXCLUDED.o,
    h=EXCLUDED.h,
    l=EXCLUDED.l,
    c=EXCLUDED.c,
    vol=bars_1s.vol + EXCLUDED.vol,
    n_trades=bars_1s.n_trades + EXCLUDED.n_trades;
`

func main() {
	var cfg Config
	envconfig.MustProcess("", &cfg)
	logger := shared.NewLogger("bars1s")
	m := newMetrics()
	ms := shared.NewMetricsServer(cfg.Metrics.Port)
	ms.Start()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg.Kafka.InTopic = cfg.InTopic
	cfg.Kafka.OutTopic = cfg.OutTopic

	consumer, err := shared.NewConsumer(cfg.Kafka, []string{cfg.InTopic})
	if err != nil {
		logger.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	producer, err := shared.NewProducer(cfg.Kafka)
	if err != nil {
		logger.Fatalf("producer init: %v", err)
	}
	defer producer.Close()

	db, err := shared.NewPgxPool(ctx, cfg.PG)
	if err != nil {
		logger.Fatalf("db init: %v", err)
	}
	defer db.Close()

	bars := make(map[string]*BarState)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush(ctx, bars, cfg, producer, db, m, true)
		default:
			msg, err := consumer.Poll(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}
			var tk shared.Tick
			if err := json.Unmarshal(msg.Value, &tk); err != nil {
				continue
			}
			m.ticks.Inc()
			sec := tk.EventTS / 1_000_000_000
			b := bars[tk.Symbol]
			if b == nil {
				b = &BarState{Sec: sec}
				bars[tk.Symbol] = b
			}
			if sec != b.Sec {
				flushSymbol(ctx, tk.Symbol, b, cfg, producer, db, m, false)
				b = &BarState{Sec: sec}
				bars[tk.Symbol] = b
			}
			b.Update(tk.LTP, tk.Vol)
			m.openBars.Set(float64(len(bars)))
			_ = shared.CommitSingle(consumer, msg)
		}
	}
}

func flush(ctx context.Context, bars map[string]*BarState, cfg Config, prod shared.Producer, db *shared.PgxDB, m metrics, forceOld bool) {
	start := time.Now()
	for sym, bar := range bars {
		if forceOld || time.Now().Unix()-bar.Sec >= int64(cfg.Grace.FlushGrace.Seconds()) {
			flushSymbol(ctx, sym, bar, cfg, prod, db, m, true)
			delete(bars, sym)
		}
	}
	m.flushDur.Observe(time.Since(start).Seconds())
	m.openBars.Set(float64(len(bars)))
}

func flushSymbol(ctx context.Context, sym string, bar *BarState, cfg Config, prod shared.Producer, db *shared.PgxDB, m metrics, publish bool) {
	if bar == nil || bar.NTrades == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = db.Exec(ctx, upsertSQL, sym, bar.Sec, bar.O, bar.H, bar.L, bar.C, bar.Vol, bar.NTrades)
	if publish {
		payload := shared.Bar1s{Symbol: sym, TF: "1s", TS: bar.Sec, O: bar.O, H: bar.H, L: bar.L, C: bar.C, Vol: bar.Vol, NTrades: bar.NTrades}
		m.barLat.Observe(time.Since(time.Unix(bar.Sec, 0)).Seconds())
		_ = prod.ProduceJSON(ctx, cfg.OutTopic, []byte(sym), payload)
		m.barsOut.Inc()
	}
}
