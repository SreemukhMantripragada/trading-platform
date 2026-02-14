package main

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"trading-platform/go/pkg/shared"

	"github.com/jackc/pgx/v5"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	Kafka       shared.KafkaConfig
	PG          shared.PostgresConfig
	Metrics     shared.MetricsConfig
	Grace       shared.GraceConfig
	InTopic     string `envconfig:"IN_TOPIC" default:"bars.1s"`
	OutTopic    string `envconfig:"OUT_TOPIC" default:"bars.1m"`
	Workers     int    `envconfig:"BAR1M_WORKERS" default:"8"`
	QueueSize   int    `envconfig:"BAR1M_QUEUE_SIZE" default:"8000"`
	FlushTickMS int    `envconfig:"BAR1M_FLUSH_TICK_MS" default:"500"`
}

type minuteBar struct {
	Start      int64
	O, H, L, C float64
	Vol        int64
	NTrades    int64
}

func newMinuteBar(start int64, in shared.Bar1s) minuteBar {
	return minuteBar{
		Start:   start,
		O:       in.O,
		H:       in.H,
		L:       in.L,
		C:       in.C,
		Vol:     in.Vol,
		NTrades: in.NTrades,
	}
}

func (b *minuteBar) Update(in shared.Bar1s) {
	if b.NTrades == 0 {
		b.O = in.O
		b.H = in.H
		b.L = in.L
		b.C = in.C
		b.Vol = in.Vol
		b.NTrades = in.NTrades
		return
	}
	if in.H > b.H {
		b.H = in.H
	}
	if in.L < b.L {
		b.L = in.L
	}
	b.C = in.C
	b.Vol += in.Vol
	b.NTrades += in.NTrades
}

type metrics struct {
	barsIn    prometheus.Counter
	barsOut   prometheus.Counter
	barsWrite prometheus.Counter
	flushDur  prometheus.Histogram
	barLat    prometheus.Histogram
	active    prometheus.Gauge
}

func newMetrics() metrics {
	return metrics{
		barsIn:    shared.NewCounter(prometheus.CounterOpts{Name: "bars_1m_input_total", Help: "1m aggregator input bars"}),
		barsOut:   shared.NewCounter(prometheus.CounterOpts{Name: "bars_1m_published_total", Help: "1m bars published"}),
		barsWrite: shared.NewCounter(prometheus.CounterOpts{Name: "bars_1m_written_total", Help: "1m bars written"}),
		flushDur: shared.NewHist(prometheus.HistogramOpts{
			Name:    "bars_1m_flush_seconds",
			Help:    "1m flush duration",
			Buckets: []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0},
		}),
		barLat: shared.NewHist(prometheus.HistogramOpts{
			Name:    "bars_1m_publish_latency_seconds",
			Help:    "Latency between 1m bar close and publish",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20},
		}),
		active: shared.NewGauge(prometheus.GaugeOpts{Name: "bars_1m_active_windows", Help: "Open 1m windows"}),
	}
}

type flushRow struct {
	symbol string
	bar    minuteBar
}

type worker struct {
	id       int
	cfg      Config
	db       *shared.PgxDB
	producer shared.Producer
	metrics  metrics
	active   *atomic.Int64
	log      shared.Logger
	in       chan shared.Bar1s
	state    map[string]minuteBar
}

func (w *worker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer w.producer.Close()

	flushTick := time.Duration(maxInt(w.cfg.FlushTickMS, 100)) * time.Millisecond
	ticker := time.NewTicker(flushTick)
	defer ticker.Stop()
	done := ctx.Done()

	for {
		select {
		case bar, ok := <-w.in:
			if !ok {
				w.flushDue(true)
				return
			}
			w.handleBar(bar)
		case <-ticker.C:
			w.flushDue(false)
		case <-done:
			// Stop selecting on ctx.Done to avoid busy-looping; channel close will drain.
			done = nil
		}
	}
}

func (w *worker) handleBar(in shared.Bar1s) {
	if in.Symbol == "" {
		return
	}
	w.metrics.barsIn.Inc()
	sec := in.TS
	if sec <= 0 {
		sec = time.Now().Unix()
	}
	minuteStart := sec - (sec % 60)

	cur, ok := w.state[in.Symbol]
	if !ok {
		w.state[in.Symbol] = newMinuteBar(minuteStart, in)
		w.active.Add(1)
		w.metrics.active.Set(float64(w.active.Load()))
		return
	}
	if cur.Start == minuteStart {
		cur.Update(in)
		w.state[in.Symbol] = cur
		return
	}

	done := flushRow{symbol: in.Symbol, bar: cur}
	w.state[in.Symbol] = newMinuteBar(minuteStart, in)
	w.flushBatch([]flushRow{done})
}

func (w *worker) flushDue(force bool) {
	if len(w.state) == 0 {
		return
	}
	now := time.Now().Unix()
	grace := int64(w.cfg.Grace.FlushGrace.Seconds())
	if grace < 0 {
		grace = 0
	}
	out := make([]flushRow, 0, len(w.state))
	for sym, bar := range w.state {
		if !force && now < bar.Start+60+grace {
			continue
		}
		out = append(out, flushRow{symbol: sym, bar: bar})
		delete(w.state, sym)
		w.active.Add(-1)
	}
	if len(out) > 0 {
		w.flushBatch(out)
	}
	w.metrics.active.Set(float64(w.active.Load()))
}

func (w *worker) flushBatch(batch []flushRow) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	if err := w.writeBatch(ctx, batch); err != nil {
		w.log.Printf("[baragg1m] worker=%d db batch write failed: %v", w.id, err)
		w.metrics.flushDur.Observe(time.Since(start).Seconds())
		return
	}
	w.metrics.barsWrite.Add(float64(len(batch)))

	if w.cfg.OutTopic != "" {
		records := make([]shared.Record, 0, len(batch))
		published := make([]flushRow, 0, len(batch))
		for _, row := range batch {
			payload := shared.Bar1s{
				Symbol:  row.symbol,
				TF:      "1m",
				TS:      row.bar.Start,
				O:       row.bar.O,
				H:       row.bar.H,
				L:       row.bar.L,
				C:       row.bar.C,
				Vol:     row.bar.Vol,
				NTrades: row.bar.NTrades,
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				continue
			}
			records = append(records, shared.Record{
				Key:   []byte(row.symbol),
				Value: raw,
				Time:  time.Now().UTC(),
			})
			published = append(published, row)
		}
		if len(records) > 0 {
			if err := w.producer.ProduceBatch(ctx, w.cfg.OutTopic, records); err != nil {
				w.log.Printf("[baragg1m] worker=%d kafka publish failed: %v", w.id, err)
			} else {
				w.metrics.barsOut.Add(float64(len(published)))
				for _, row := range published {
					w.metrics.barLat.Observe(time.Since(time.Unix(row.bar.Start+60, 0)).Seconds())
				}
			}
		}
	}
	w.metrics.flushDur.Observe(time.Since(start).Seconds())
}

func (w *worker) writeBatch(ctx context.Context, batch []flushRow) error {
	conn, err := w.db.Acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	pgBatch := &pgx.Batch{}
	for _, row := range batch {
		bar := row.bar
		pgBatch.Queue(
			upsertSQL,
			row.symbol,
			bar.Start,
			bar.O,
			bar.H,
			bar.L,
			bar.C,
			bar.Vol,
			bar.NTrades,
		)
	}
	br := conn.SendBatch(ctx, pgBatch)
	defer br.Close()

	for range batch {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

func startWorkers(
	ctx context.Context,
	cfg Config,
	db *shared.PgxDB,
	m metrics,
	log shared.Logger,
	active *atomic.Int64,
) ([]chan shared.Bar1s, func(), error) {
	workers := maxInt(cfg.Workers, 1)
	queueSize := maxInt(cfg.QueueSize, 1)
	chans := make([]chan shared.Bar1s, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		p, err := shared.NewProducer(cfg.Kafka)
		if err != nil {
			for j := 0; j < i; j++ {
				close(chans[j])
			}
			wg.Wait()
			return nil, nil, err
		}
		ch := make(chan shared.Bar1s, queueSize)
		chans[i] = ch
		w := &worker{
			id:       i,
			cfg:      cfg,
			db:       db,
			producer: p,
			metrics:  m,
			active:   active,
			log:      log,
			in:       ch,
			state:    make(map[string]minuteBar),
		}
		wg.Add(1)
		go w.run(ctx, &wg)
	}

	stop := func() {
		for _, ch := range chans {
			close(ch)
		}
		wg.Wait()
	}
	return chans, stop, nil
}

func symbolShard(symbol string, workers int) int {
	if workers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(symbol))
	return int(h.Sum32() % uint32(workers))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

const upsertSQL = `
INSERT INTO bars_1m(symbol, ts, o, h, l, c, vol, n_trades)
VALUES($1, to_timestamp($2), $3, $4, $5, $6, $7, $8)
ON CONFLICT(symbol, ts) DO UPDATE
SET o = EXCLUDED.o,
    h = GREATEST(bars_1m.h, EXCLUDED.h),
    l = LEAST(bars_1m.l, EXCLUDED.l),
    c = EXCLUDED.c,
    vol = bars_1m.vol + EXCLUDED.vol,
    n_trades = bars_1m.n_trades + EXCLUDED.n_trades;
`

func main() {
	var cfg Config
	envconfig.MustProcess("", &cfg)
	logger := shared.NewLogger("baragg1m")
	m := newMetrics()
	ms := shared.NewMetricsServer(cfg.Metrics.Port)
	ms.Start()

	ctx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSig()

	cfg.Kafka.InTopic = cfg.InTopic
	cfg.Kafka.OutTopic = cfg.OutTopic

	consumer, err := shared.NewConsumer(cfg.Kafka, []string{cfg.InTopic})
	if err != nil {
		logger.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	db, err := shared.NewPgxPool(ctx, cfg.PG)
	if err != nil {
		logger.Fatalf("db init: %v", err)
	}
	defer db.Close()

	var active atomic.Int64
	workerChans, stopWorkers, err := startWorkers(ctx, cfg, db, m, logger, &active)
	if err != nil {
		logger.Fatalf("worker init: %v", err)
	}
	defer stopWorkers()

	logger.Printf(
		"running 1m aggregator in=%s out=%s workers=%d queue=%d",
		cfg.InTopic,
		cfg.OutTopic,
		maxInt(cfg.Workers, 1),
		maxInt(cfg.QueueSize, 1),
	)

loop:
	for {
		msg, err := consumer.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break loop
			}
			continue
		}

		var bar shared.Bar1s
		if err := json.Unmarshal(msg.Value, &bar); err != nil {
			_ = shared.CommitSingle(consumer, msg)
			continue
		}
		idx := symbolShard(bar.Symbol, len(workerChans))
		select {
		case workerChans[idx] <- bar:
		case <-ctx.Done():
			break loop
		}
		if err := shared.CommitSingle(consumer, msg); err != nil {
			logger.Printf("[baragg1m] commit failed: %v", err)
		}
	}
	logger.Printf("1m aggregator shutdown: draining worker queues")
}
