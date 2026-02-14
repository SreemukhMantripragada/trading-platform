package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
	OutTFs      string `envconfig:"OUT_TFS" default:"3,5,15"`
	OutTopic    string `envconfig:"OUT_TOPIC_PREFIX" default:"bars"`
	Workers     int    `envconfig:"BARAGG_WORKERS" default:"8"`
	QueueSize   int    `envconfig:"BARAGG_QUEUE_SIZE" default:"8000"`
	FlushTickMS int    `envconfig:"BARAGG_FLUSH_TICK_MS" default:"500"`
}

// TimeframeWindow aggregates bars for a single symbol and tf.
type TimeframeWindow struct {
	Start           int64
	O, H, L, C      float64
	Vol             int64
	NTrades         int64
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

type metrics struct {
	barsIn   prometheus.Counter
	barsOut  *prometheus.CounterVec
	active   *prometheus.GaugeVec
	flushDur prometheus.Histogram
	latency  *prometheus.HistogramVec
}

func newMetrics(tfs []int) metrics {
	m := metrics{
		barsIn:   shared.NewCounter(prometheus.CounterOpts{Name: "baragg_input_total", Help: "1s bars in"}),
		barsOut:  shared.NewCounterVec(prometheus.CounterOpts{Name: "baragg_output_total", Help: "Bars out"}, []string{"tf"}),
		active:   shared.NewGaugeVec(prometheus.GaugeOpts{Name: "baragg_active_windows", Help: "Active windows"}, []string{"tf"}),
		flushDur: shared.NewHist(prometheus.HistogramOpts{Name: "baragg_flush_seconds", Help: "Flush duration", Buckets: []float64{0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5}}),
		latency:  shared.NewHistVec(prometheus.HistogramOpts{Name: "baragg_bar_latency_seconds", Help: "Close->publish latency", Buckets: []float64{0.5, 1, 2, 5, 10, 20}}, []string{"tf"}),
	}
	for _, tf := range tfs {
		m.active.WithLabelValues(fmt.Sprintf("%dm", tf)).Set(0)
	}
	return m
}

type aggOutput struct {
	symbol string
	tf     int
	win    TimeframeWindow
}

type worker struct {
	id       int
	cfg      Config
	tfs      []int
	db       *shared.PgxDB
	producer shared.Producer
	metrics  metrics
	log      shared.Logger
	in       chan shared.Bar1s
	state    map[int]map[string]*TimeframeWindow
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
			// Stop selecting on ctx.Done to avoid busy-looping; channel close will finish drain.
			done = nil
		}
	}
}

func (w *worker) handleBar(bar shared.Bar1s) {
	if bar.Symbol == "" {
		return
	}
	w.metrics.barsIn.Inc()
	done := make([]aggOutput, 0, len(w.tfs))
	for _, tf := range w.tfs {
		bucket := bucketStart(bar.TS, tf)
		perTF := w.state[tf]
		cur, ok := perTF[bar.Symbol]
		if !ok {
			perTF[bar.Symbol] = &TimeframeWindow{Start: bucket}
			w.metrics.active.WithLabelValues(fmt.Sprintf("%dm", tf)).Inc()
			cur = perTF[bar.Symbol]
		}
		if cur.Start != bucket {
			done = append(done, aggOutput{symbol: bar.Symbol, tf: tf, win: *cur})
			perTF[bar.Symbol] = &TimeframeWindow{Start: bucket}
			cur = perTF[bar.Symbol]
		}
		cur.Update(bar.O, bar.H, bar.L, bar.C, bar.Vol, bar.NTrades)
	}
	if len(done) > 0 {
		w.flushBatch(done)
	}
}

func (w *worker) flushDue(force bool) {
	out := make([]aggOutput, 0)
	now := time.Now().Unix()
	graceSec := int64(w.cfg.Grace.FlushGrace.Seconds())
	if graceSec < 0 {
		graceSec = 0
	}
	for _, tf := range w.tfs {
		perTF := w.state[tf]
		width := int64(tf * 60)
		label := fmt.Sprintf("%dm", tf)
		for sym, win := range perTF {
			if !force && now < win.Start+width+graceSec {
				continue
			}
			out = append(out, aggOutput{
				symbol: sym,
				tf:     tf,
				win:    *win,
			})
			delete(perTF, sym)
			w.metrics.active.WithLabelValues(label).Dec()
		}
	}
	if len(out) > 0 {
		w.flushBatch(out)
	}
}

func (w *worker) flushBatch(batch []aggOutput) {
	if len(batch) == 0 {
		return
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	if err := w.writeBatch(ctx, batch); err != nil {
		w.log.Printf("[baragg] worker=%d db batch write failed: %v", w.id, err)
		w.metrics.flushDur.Observe(time.Since(start).Seconds())
		return
	}

	byTF := make(map[int][]aggOutput)
	for _, row := range batch {
		byTF[row.tf] = append(byTF[row.tf], row)
	}

	for tf, rows := range byTF {
		records := make([]shared.Record, 0, len(rows))
		for _, row := range rows {
			payload := shared.BarTF{
				Symbol:  row.symbol,
				TS:      row.win.Start,
				O:       row.win.O,
				H:       row.win.H,
				L:       row.win.L,
				C:       row.win.C,
				Vol:     row.win.Vol,
				NTrades: row.win.NTrades,
			}
			raw, err := json.Marshal(payload)
			if err != nil {
				continue
			}
			records = append(records, shared.Record{
				Key:   []byte(fmt.Sprintf("%s:%d", row.symbol, row.win.Start)),
				Value: raw,
				Time:  time.Now().UTC(),
			})
		}
		if len(records) == 0 {
			continue
		}
		topic := fmt.Sprintf("%s.%dm", w.cfg.OutTopic, tf)
		if err := w.producer.ProduceBatch(ctx, topic, records); err != nil {
			w.log.Printf("[baragg] worker=%d kafka publish failed tf=%d: %v", w.id, tf, err)
			continue
		}
		label := fmt.Sprintf("%dm", tf)
		w.metrics.barsOut.WithLabelValues(label).Add(float64(len(records)))
		for _, row := range rows {
			w.metrics.latency.WithLabelValues(label).Observe(time.Since(time.Unix(row.win.Start+int64(tf*60), 0)).Seconds())
		}
	}
	w.metrics.flushDur.Observe(time.Since(start).Seconds())
}

func (w *worker) writeBatch(ctx context.Context, batch []aggOutput) error {
	conn, err := w.db.Acquire()
	if err != nil {
		return err
	}
	defer conn.Release()

	pgBatch := &pgx.Batch{}
	for _, row := range batch {
		upsert := fmt.Sprintf(upsertTmpl, row.tf, row.tf, row.tf, row.tf)
		pgBatch.Queue(
			upsert,
			row.symbol,
			row.win.Start,
			row.win.O,
			row.win.H,
			row.win.L,
			row.win.C,
			row.win.Vol,
			row.win.NTrades,
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
	tfs []int,
	db *shared.PgxDB,
	m metrics,
	log shared.Logger,
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
		state := make(map[int]map[string]*TimeframeWindow, len(tfs))
		for _, tf := range tfs {
			state[tf] = make(map[string]*TimeframeWindow)
		}
		w := &worker{
			id:       i,
			cfg:      cfg,
			tfs:      tfs,
			db:       db,
			producer: p,
			metrics:  m,
			log:      log,
			in:       ch,
			state:    state,
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

	ctx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSig()

	cfg.Kafka.InTopic = cfg.InTopic

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

	workerChans, stopWorkers, err := startWorkers(ctx, cfg, tfs, db, m, logger)
	if err != nil {
		logger.Fatalf("worker init: %v", err)
	}
	defer stopWorkers()

	logger.Printf(
		"running aggregator in=%s out_prefix=%s tfs=%v workers=%d queue=%d",
		cfg.InTopic,
		cfg.OutTopic,
		tfs,
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
			logger.Printf("[baragg] commit failed: %v", err)
		}
	}
	logger.Printf("aggregator shutdown: draining worker queues")
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
