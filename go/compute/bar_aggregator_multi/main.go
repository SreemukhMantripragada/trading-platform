package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"trading-platform/go/pkg/shared"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	Kafka    shared.KafkaConfig
	PG       shared.PostgresConfig
	Metrics  shared.MetricsConfig
	Grace    shared.GraceConfig
	InTopic  string `envconfig:"IN_TOPIC" default:"bars.1s"`
	OutTFs   string `envconfig:"OUT_TFS" default:"3,5,15"`
}

// TimeframeWindow aggregates bars for a single symbol and tf.
type TimeframeWindow struct {
	Start   int64
	O, H, L, C float64
	Vol     int64
	NTrades int64
}

func (w *TimeframeWindow) Update(o, h, l, c float64, vol, n int64) {
	if w.NTrades == 0 {
		w.O, w.H, w.L, w.C = o, h, l, c
		w.Vol = vol
		w.NTrades = n
		return
	}
	if h > w.H {
		w.H = h
	}
	if l < w.L {
		w.L = l
	}
	w.C = c
	w.Vol += vol
	w.NTrades += n
}

// Metrics bundle.
type metrics struct {
	barsIn   prometheus.Counter
	barsOut  map[int]*prometheus.CounterVec
	active   *prometheus.GaugeVec
	flushDur prometheus.Histogram
	latency  map[int]prometheus.Histogram
}

func newMetrics(tfs []int) metrics {
	m := metrics{
		barsIn:   shared.NewCounter(prometheus.CounterOpts{Name: "baragg_input_total", Help: "1s bars in"}),
		barsOut:  make(map[int]*prometheus.CounterVec),
		active:   shared.NewGaugeVec(prometheus.GaugeOpts{Name: "baragg_active_windows", Help: "Active windows"}, []string{"tf"}),
		flushDur: shared.NewHist(prometheus.HistogramOpts{Name: "baragg_flush_seconds", Help: "Flush duration", Buckets: []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5}}),
		latency:  make(map[int]prometheus.Histogram),
	}
	for _, tf := range tfs {
		label := fmt.Sprintf("%dm", tf)
		m.barsOut[tf] = shared.NewCounterVec(prometheus.CounterOpts{Name: "baragg_output_total", Help: "Bars out"}, []string{"tf"})
		m.latency[tf] = shared.NewHist(prometheus.HistogramOpts{Name: "baragg_bar_latency_seconds", Help: "Close->publish latency", Buckets: []float64{0.5, 1, 2, 5, 10, 20}})
		m.active.WithLabelValues(label).Set(0)
	}
	return m
}

const upsertTmpl = `
INSERT INTO bars_%dm (symbol, ts, o, h, l, c, vol, n_trades)
VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT (symbol, ts) DO UPDATE
SET h = GREATEST(excluded.h, bars_%dm.h),
    l = LEAST(excluded.l, bars_%dm.l),
    c = excluded.c,
    vol = bars_%dm.vol + excluded.vol,
    n_trades = bars_%dm.n_trades + excluded.n_trades;
`

func main() {
	var cfg Config
	envconfig.MustProcess("", &cfg)
	logger := shared.NewLogger("baragg")

	tfs := parseTFs(cfg.OutTFs)
	m := newMetrics(tfs)
	ms := shared.NewMetricsServer(cfg.Metrics.Port)
	ms.Start()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg.Kafka.InTopic = cfg.InTopic

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

	// State: map[tf]map[symbol]*TimeframeWindow
	state := make(map[int]map[string]*TimeframeWindow)
	for _, tf := range tfs {
		state[tf] = make(map[string]*TimeframeWindow)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushAll(ctx, state, tfs, cfg, producer, db, m, true)
		default:
			msg, err := consumer.Poll(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}
			var bar shared.Bar1s
			if err := json.Unmarshal(msg.Value, &bar); err != nil {
				continue
			}
			m.barsIn.Inc()
			for _, tf := range tfs {
				bucket := bucketStart(bar.TS, tf)
				win := state[tf][bar.Symbol]
				if win == nil {
					win = &TimeframeWindow{Start: bucket}
					state[tf][bar.Symbol] = win
				}
				if win.Start != bucket {
					emit(ctx, bar.Symbol, win, tf, cfg, producer, db, m)
					win = &TimeframeWindow{Start: bucket}
					state[tf][bar.Symbol] = win
				}
				win.Update(bar.O, bar.H, bar.L, bar.C, bar.Vol, bar.NTrades)
			}
			_ = shared.CommitSingle(consumer, msg)
		}
	}
}

func flushAll(ctx context.Context, state map[int]map[string]*TimeframeWindow, tfs []int, cfg Config, prod shared.Producer, db *shared.PgxDB, m metrics, force bool) {
	start := time.Now()
	now := time.Now().Unix()
	for _, tf := range tfs {
		for sym, win := range state[tf] {
			width := int64(tf * 60)
			if force || now >= win.Start+width+int64(cfg.Grace.FlushGrace.Seconds()) {
				emit(ctx, sym, win, tf, cfg, prod, db, m)
				delete(state[tf], sym)
			}
		}
		m.active.WithLabelValues(fmt.Sprintf("%dm", tf)).Set(float64(len(state[tf])))
	}
	m.flushDur.Observe(time.Since(start).Seconds())
}

func emit(ctx context.Context, sym string, win *TimeframeWindow, tf int, cfg Config, prod shared.Producer, db *shared.PgxDB, m metrics) {
	if win == nil || win.NTrades == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	upsert := fmt.Sprintf(upsertTmpl, tf, tf, tf, tf)
	_ = db.Exec(ctx, upsert, sym, win.Start, win.O, win.H, win.L, win.C, win.Vol, win.NTrades)
	payload := shared.BarTF{Symbol: sym, TS: win.Start, O: win.O, H: win.H, L: win.L, C: win.C, Vol: win.Vol, NTrades: win.NTrades}
	topic := fmt.Sprintf("bars.%dm", tf)
	m.latency[tf].Observe(time.Since(time.Unix(win.Start+int64(tf*60), 0)).Seconds())
	_ = prod.ProduceJSON(ctx, topic, []byte(fmt.Sprintf("%s:%d", sym, win.Start)), payload)
	m.barsOut[tf].WithLabelValues(fmt.Sprintf("%dm", tf)).Inc()
}

func bucketStart(ts int64, tf int) int64 {
	width := int64(tf * 60)
	return ts - (ts % width)
}

func parseTFs(raw string) []int {
	parts := strings.Split(raw, ",")
	out := []int{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return []int{3, 5, 15}
	}
	return out
}
