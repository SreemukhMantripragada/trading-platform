package main

import (
	"container/heap"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"trading-platform/go/pkg/shared"

	kitemodels "github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

// Config specific to ingestion.
type Config struct {
	Kafka          shared.KafkaConfig
	Metrics        shared.MetricsConfig
	TokensCSV      string  `envconfig:"ZERODHA_TOKENS_CSV" default:"configs/tokens.csv"`
	TokenJSON      string  `envconfig:"ZERODHA_TOKEN_FILE" default:"ingestion/auth/token.json"`
	APIKey         string  `envconfig:"KITE_API_KEY"`
	AccessToken    string  `envconfig:"KITE_ACCESS_TOKEN"` // optional override
	TickTopic      string  `envconfig:"TICKS_TOPIC" default:"ticks"`
	SimTicks       bool    `envconfig:"SIM_TICKS" default:"false"`
	SimBaseTPS     float64 `envconfig:"SIM_BASE_TPS" default:"10.0"`
	SimHotTPS      float64 `envconfig:"SIM_HOT_TPS" default:"1000.0"`
	SimHotPct      float64 `envconfig:"SIM_HOT_SYMBOL_PCT" default:"0.0"`
	SimHotRotate   int     `envconfig:"SIM_HOT_ROTATE_SEC" default:"15"`
	SimStepMs      int     `envconfig:"SIM_STEP_MS" default:"100"`
	SimBasePrice   float64 `envconfig:"SIM_BASE_PRICE" default:"2500.0"`
	SimVolMin      int64   `envconfig:"SIM_VOL_MIN" default:"1"`
	SimVolMax      int64   `envconfig:"SIM_VOL_MAX" default:"5"`
	BatchFlushMs   int     `envconfig:"BATCH_FLUSH_MS" default:"200"`
	MaxBatch       int     `envconfig:"MAX_BATCH" default:"256"`
	ProduceWorkers int     `envconfig:"PRODUCE_WORKERS" default:"8"`
	ProduceQueue   int     `envconfig:"PRODUCE_QUEUE" default:"16000"`
}

// TickSource emits ticks.
type TickSource interface {
	Start(ctx context.Context, out chan<- shared.Tick) error
}

// KiteWSSource streams real ticks via Zerodha websocket.
type KiteWSSource struct {
	apiKey      string
	accessToken string
	tokens      []uint32
	tokenToSym  map[uint32]string
	log         shared.Logger
	metrics     ingestMetrics
}

func (k *KiteWSSource) Start(ctx context.Context, out chan<- shared.Tick) error {
	if len(k.tokens) == 0 {
		return errors.New("no tokens to subscribe")
	}
	t := kiteticker.New(k.apiKey, k.accessToken)

	// callbacks
	t.OnError(func(err error) {
		k.log.Printf("[ws] error: %v", err)
		k.metrics.wsEvents.WithLabelValues("error").Inc()
	})
	t.OnClose(func(code int, reason string) {
		k.log.Printf("[ws] closed %d %s", code, reason)
		k.metrics.wsEvents.WithLabelValues("close").Inc()
	})
	t.OnReconnect(func(attempt int, delay time.Duration) {
		k.log.Printf("[ws] reconnecting attempt=%d delay=%s", attempt, delay)
		k.metrics.wsEvents.WithLabelValues("reconnect").Inc()
	})
	t.OnConnect(func() {
		k.log.Printf("[ws] connected; subscribing %d tokens", len(k.tokens))
		k.metrics.wsEvents.WithLabelValues("connect").Inc()
		for _, chunk := range chunkTokens(k.tokens, 200) {
			if err := t.Subscribe(chunk); err != nil {
				k.log.Printf("[ws] subscribe chunk failed: %v", err)
			}
			if err := t.SetMode(kiteticker.ModeLTP, chunk); err != nil {
				k.log.Printf("[ws] set mode failed: %v", err)
			}
		}
	})
	t.OnNoReconnect(func(attempt int) {
		k.log.Printf("[ws] no more reconnects after attempt %d", attempt)
		k.metrics.wsEvents.WithLabelValues("noreconnect").Inc()
	})
	t.OnTick(func(tk kitemodels.Tick) {
		sym := k.tokenToSym[tk.InstrumentToken]
		if sym == "" {
			return
		}
		eventTS := tk.Timestamp.Time
		var ns int64
		if eventTS.IsZero() {
			ns = time.Now().UnixNano()
		} else {
			ns = eventTS.UnixNano()
		}
		outTick := shared.Tick{
			Symbol:  sym,
			EventTS: ns,
			LTP:     tk.LastPrice,
			Vol:     int64(tk.VolumeTraded),
		}
		select {
		case out <- outTick:
		default:
			k.metrics.dropped.Inc()
		}
	})

	go func() {
		<-ctx.Done()
		t.Stop()
	}()
	go t.ServeWithContext(ctx)
	return nil
}

// SimSource emits synthetic ticks for testing.
type SimSource struct {
	symbols   []string
	baseTPS   float64
	hotTPS    float64
	hotPct    float64
	hotRotate time.Duration
	step      time.Duration
	basePrice float64
	volMin    int64
	volMax    int64
}

type simSchedule struct {
	symbol string
	due    time.Time
}

type simScheduleHeap []simSchedule

func (h simScheduleHeap) Len() int { return len(h) }

func (h simScheduleHeap) Less(i, j int) bool {
	return h[i].due.Before(h[j].due)
}

func (h simScheduleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *simScheduleHeap) Push(x any) {
	*h = append(*h, x.(simSchedule))
}

func (h *simScheduleHeap) Pop() any {
	old := *h
	n := len(old)
	out := old[n-1]
	*h = old[:n-1]
	return out
}

func sampleGap(rateTPS float64, rng *rand.Rand) time.Duration {
	if rateTPS <= 0 {
		return time.Second
	}
	sec := rng.ExpFloat64() / rateTPS
	if sec < 0.0005 {
		sec = 0.0005
	}
	return time.Duration(sec * float64(time.Second))
}

func symbolRate(sym string, hot map[string]struct{}, baseTPS, hotTPS float64) float64 {
	if _, ok := hot[sym]; ok {
		return hotTPS
	}
	return baseTPS
}

func (s *SimSource) Start(ctx context.Context, out chan<- shared.Tick) error {
	if len(s.symbols) == 0 {
		s.symbols = []string{"SIM"}
	}
	if s.step <= 0 {
		s.step = 20 * time.Millisecond
	}
	if s.basePrice <= 0 {
		s.basePrice = 2500.0
	}
	if s.volMin <= 0 {
		s.volMin = 1
	}
	if s.volMax < s.volMin {
		s.volMax = s.volMin
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	prices := make(map[string]float64, len(s.symbols))
	for _, sym := range s.symbols {
		prices[sym] = s.basePrice + (rng.Float64()*10.0 - 5.0)
	}
	hot := pickHotSymbols(s.symbols, s.hotPct, rng)
	nextRotate := time.Now().Add(s.hotRotate)
	sched := make(simScheduleHeap, 0, len(s.symbols))
	now := time.Now()
	for _, sym := range s.symbols {
		rate := symbolRate(sym, hot, s.baseTPS, s.hotTPS)
		jitter := time.Duration(rng.Float64() * float64(time.Second))
		heap.Push(&sched, simSchedule{symbol: sym, due: now.Add(jitter + sampleGap(rate, rng))})
	}

	timer := time.NewTimer(1 * time.Millisecond)
	go func() {
		defer timer.Stop()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				now = time.Now()
				if s.hotRotate > 0 && now.After(nextRotate) {
					hot = pickHotSymbols(s.symbols, s.hotPct, rng)
					nextRotate = now.Add(s.hotRotate)
				}

				emitted := 0
				for sched.Len() > 0 {
					head := sched[0]
					if head.due.After(now) {
						break
					}
					item := heap.Pop(&sched).(simSchedule)
					sym := item.symbol
					drift := rng.Float64()*0.8 - 0.4
					price := prices[sym] + drift
					if price < 1.0 {
						price = 1.0
					}
					prices[sym] = price
					vol := s.volMin
					if s.volMax > s.volMin {
						vol += rng.Int63n(s.volMax - s.volMin + 1)
					}

					tick := shared.Tick{
						Symbol:  sym,
						EventTS: time.Now().UnixNano(),
						LTP:     price,
						Vol:     vol,
					}
					select {
					case <-ctx.Done():
						return
					case out <- tick:
					}

					rate := symbolRate(sym, hot, s.baseTPS, s.hotTPS)
					item.due = time.Now().Add(sampleGap(rate, rng))
					heap.Push(&sched, item)
					emitted++
					if emitted >= 2048 {
						break
					}
					now = time.Now()
				}

				wait := s.step
				if wait <= 0 {
					wait = 20 * time.Millisecond
				}
				if sched.Len() > 0 {
					nextDue := time.Until(sched[0].due)
					if nextDue < wait {
						wait = nextDue
					}
				}
				if s.hotRotate > 0 {
					toRotate := time.Until(nextRotate)
					if toRotate < wait {
						wait = toRotate
					}
				}
				if wait < time.Millisecond {
					wait = time.Millisecond
				}
				if wait > 50*time.Millisecond {
					wait = 50 * time.Millisecond
				}
				timer.Reset(wait)
			}
		}
	}()
	return nil
}

func pickHotSymbols(symbols []string, pct float64, rng *rand.Rand) map[string]struct{} {
	out := map[string]struct{}{}
	if pct <= 0 || len(symbols) == 0 {
		return out
	}
	want := int(float64(len(symbols)) * pct)
	if want <= 0 {
		want = 1
	}
	if want >= len(symbols) {
		for _, sym := range symbols {
			out[sym] = struct{}{}
		}
		return out
	}
	perm := rng.Perm(len(symbols))
	for i := 0; i < want; i++ {
		out[symbols[perm[i]]] = struct{}{}
	}
	return out
}

// Metrics bundle.
type ingestMetrics struct {
	ticksOut *prometheus.CounterVec
	qDepth   prometheus.Gauge
	batchSz  prometheus.Histogram
	latency  prometheus.Histogram
	wsEvents *prometheus.CounterVec
	dropped  prometheus.Counter
}

func newMetrics() ingestMetrics {
	return ingestMetrics{
		ticksOut: shared.NewCounterVec(prometheus.CounterOpts{Name: "ingest_ticks_total", Help: "Ticks emitted"}, []string{"symbol"}),
		qDepth:   shared.NewGauge(prometheus.GaugeOpts{Name: "ingest_queue_depth", Help: "Ticks queued"}),
		batchSz:  shared.NewHist(prometheus.HistogramOpts{Name: "ingest_batch_size", Help: "Batch size", Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500}}),
		latency:  shared.NewHist(prometheus.HistogramOpts{Name: "ingest_latency_seconds", Help: "Event to publish latency", Buckets: []float64{0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5}}),
		wsEvents: shared.NewCounterVec(prometheus.CounterOpts{Name: "ingest_ws_events_total", Help: "Websocket lifecycle events"}, []string{"event"}),
		dropped:  shared.NewCounter(prometheus.CounterOpts{Name: "ingest_ticks_dropped_total", Help: "Ticks dropped due to full queue"}),
	}
}

func startProducerWorkers(
	ctx context.Context,
	cfg Config,
	logger shared.Logger,
	metrics ingestMetrics,
	inFlight *atomic.Int64,
) ([]chan shared.Tick, func(), error) {
	workers := cfg.ProduceWorkers
	if workers < 1 {
		workers = 1
	}
	queueDepth := cfg.ProduceQueue
	if queueDepth < 1 {
		queueDepth = 1000
	}
	maxBatch := cfg.MaxBatch
	if maxBatch < 1 {
		maxBatch = 256
	}
	flushEvery := time.Duration(cfg.BatchFlushMs) * time.Millisecond
	if flushEvery <= 0 {
		flushEvery = 50 * time.Millisecond
	}

	chans := make([]chan shared.Tick, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		ch := make(chan shared.Tick, queueDepth)
		chans[i] = ch
		producer, err := shared.NewProducer(cfg.Kafka)
		if err != nil {
			for j := 0; j < i; j++ {
				close(chans[j])
			}
			wg.Wait()
			return nil, nil, err
		}
		wg.Add(1)
		go func(workerID int, in <-chan shared.Tick, p shared.Producer) {
			defer wg.Done()
			defer p.Close()
			batch := make([]shared.Tick, 0, maxBatch)
			timer := time.NewTimer(flushEvery)
			defer timer.Stop()
			done := ctx.Done()

			flush := func() {
				if len(batch) == 0 {
					return
				}
				metrics.batchSz.Observe(float64(len(batch)))
				records := make([]shared.Record, 0, len(batch))
				validTicks := make([]shared.Tick, 0, len(batch))
				marshalDrops := 0
				for _, tk := range batch {
					raw, err := json.Marshal(tk)
					if err != nil {
						marshalDrops++
						continue
					}
					records = append(records, shared.Record{
						Key:   []byte(tk.Symbol),
						Value: raw,
						Time:  time.Now().UTC(),
					})
					validTicks = append(validTicks, tk)
				}

				if marshalDrops > 0 {
					metrics.dropped.Add(float64(marshalDrops))
				}
				if len(records) > 0 {
					writeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					err := p.ProduceBatch(writeCtx, cfg.TickTopic, records)
					cancel()
					if err != nil {
						metrics.dropped.Add(float64(len(records)))
						logger.Printf("[ingest] producer worker=%d batch write failed: %v", workerID, err)
					} else {
						for _, tk := range validTicks {
							metrics.latency.Observe(time.Since(time.Unix(0, tk.EventTS)).Seconds())
							metrics.ticksOut.WithLabelValues(tk.Symbol).Inc()
						}
					}
				}
				inFlight.Add(int64(-len(batch)))
				batch = batch[:0]
			}

			for {
				select {
				case tk, ok := <-in:
					if !ok {
						flush()
						return
					}
					batch = append(batch, tk)
					if len(batch) >= maxBatch {
						flush()
						if !timer.Stop() {
							select {
							case <-timer.C:
							default:
							}
						}
						timer.Reset(flushEvery)
					}
				case <-timer.C:
					flush()
					timer.Reset(flushEvery)
				case <-done:
					// Stop selecting on ctx.Done to avoid busy-looping; channel close will finish drain.
					done = nil
				}
			}
		}(i, ch, producer)
	}

	stop := func() {
		for _, ch := range chans {
			close(ch)
		}
		wg.Wait()
	}
	return chans, stop, nil
}

func workerForSymbol(symbol string, workers int) int {
	if workers <= 1 {
		return 0
	}
	var h uint32 = 2166136261
	for i := 0; i < len(symbol); i++ {
		h ^= uint32(symbol[i])
		h *= 16777619
	}
	return int(h % uint32(workers))
}

func main() {
	var cfg Config
	envconfig.MustProcess("", &cfg)
	logger := shared.NewLogger("ingest")
	metrics := newMetrics()
	ms := shared.NewMetricsServer(cfg.Metrics.Port)
	ms.Start()

	ctx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSig()

	src, err := buildSource(cfg, logger, metrics)
	if err != nil {
		logger.Fatalf("build source: %v", err)
	}

	out := make(chan shared.Tick, 20000)
	if err := src.Start(ctx, out); err != nil {
		logger.Fatalf("source start: %v", err)
	}

	var inFlight atomic.Int64
	workerChans, stopWorkers, err := startProducerWorkers(ctx, cfg, logger, metrics, &inFlight)
	if err != nil {
		logger.Fatalf("producer worker init: %v", err)
	}
	defer stopWorkers()

	logger.Printf(
		"running ingestion -> topic=%s sim=%v base_tps=%.2f hot_tps=%.2f hot_pct=%.4f step_ms=%d workers=%d worker_q=%d",
		cfg.TickTopic,
		cfg.SimTicks,
		cfg.SimBaseTPS,
		cfg.SimHotTPS,
		cfg.SimHotPct,
		cfg.SimStepMs,
		cfg.ProduceWorkers,
		cfg.ProduceQueue,
	)
	qTicker := time.NewTicker(250 * time.Millisecond)
	defer qTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Printf("ingestion shutdown: draining producer queues")
			return
		case tk, ok := <-out:
			if !ok {
				logger.Printf("ingestion source closed")
				return
			}
			idx := workerForSymbol(tk.Symbol, len(workerChans))
			select {
			case workerChans[idx] <- tk:
				inFlight.Add(1)
			default:
				metrics.dropped.Inc()
			}
		case <-qTicker.C:
			queued := float64(inFlight.Load() + int64(len(out)))
			metrics.qDepth.Set(queued)
		}
	}
}

func buildSource(cfg Config, logger shared.Logger, m ingestMetrics) (TickSource, error) {
	if cfg.SimTicks {
		syms, err := readSymbols(cfg.TokensCSV)
		if err != nil {
			return nil, err
		}
		step := time.Duration(cfg.SimStepMs) * time.Millisecond
		rotate := time.Duration(cfg.SimHotRotate) * time.Second
		return &SimSource{
			symbols:   syms,
			baseTPS:   cfg.SimBaseTPS,
			hotTPS:    cfg.SimHotTPS,
			hotPct:    cfg.SimHotPct,
			hotRotate: rotate,
			step:      step,
			basePrice: cfg.SimBasePrice,
			volMin:    cfg.SimVolMin,
			volMax:    cfg.SimVolMax,
		}, nil
	}

	apiKey := cfg.APIKey
	access := cfg.AccessToken
	if apiKey == "" {
		return nil, errors.New("KITE_API_KEY required for live websocket")
	}
	if access == "" {
		var err error
		access, err = loadAccessToken(cfg.TokenJSON)
		if err != nil {
			return nil, err
		}
	}

	tokens, tokenToSym, err := loadTokens(cfg.TokensCSV)
	if err != nil {
		return nil, err
	}

	return &KiteWSSource{
		apiKey:      apiKey,
		accessToken: access,
		tokens:      tokens,
		tokenToSym:  tokenToSym,
		log:         logger,
		metrics:     m,
	}, nil
}

func readSymbols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("tokens csv empty")
	}
	colSym := -1
	for i, h := range rows[0] {
		if strings.EqualFold(h, "tradingsymbol") {
			colSym = i
			break
		}
	}
	if colSym == -1 {
		return nil, errors.New("tradingsymbol column missing")
	}
	out := make([]string, 0, len(rows)-1)
	for _, row := range rows[1:] {
		if colSym < len(row) {
			s := strings.ToUpper(strings.TrimSpace(row[colSym]))
			if s != "" {
				out = append(out, s)
			}
		}
	}
	return out, nil
}

func loadTokens(path string) ([]uint32, map[uint32]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, errors.New("tokens csv empty")
	}
	colTok, colSym := -1, -1
	for i, h := range rows[0] {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "instrument_token":
			colTok = i
		case "tradingsymbol":
			colSym = i
		}
	}
	if colTok == -1 || colSym == -1 {
		return nil, nil, errors.New("instrument_token/tradingsymbol columns required")
	}
	tokens := make([]uint32, 0, len(rows)-1)
	tokenToSym := make(map[uint32]string)
	for _, row := range rows[1:] {
		if colTok >= len(row) || colSym >= len(row) {
			continue
		}
		tokStr := strings.TrimSpace(row[colTok])
		sym := strings.ToUpper(strings.TrimSpace(row[colSym]))
		if tokStr == "" || sym == "" {
			continue
		}
		tok64, err := strconv.ParseUint(tokStr, 10, 32)
		if err != nil {
			continue
		}
		tok := uint32(tok64)
		tokens = append(tokens, tok)
		tokenToSym[tok] = sym
	}
	return tokens, tokenToSym, nil
}

func loadAccessToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("token path empty")
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err == nil {
			path = abs
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return "", err
	}
	if tok, ok := doc["access_token"].(string); ok && tok != "" {
		return tok, nil
	}
	return "", errors.New("access_token missing in token file")
}

func chunkTokens(tokens []uint32, size int) [][]uint32 {
	if size <= 0 {
		size = 200
	}
	out := [][]uint32{}
	for i := 0; i < len(tokens); i += size {
		j := i + size
		if j > len(tokens) {
			j = len(tokens)
		}
		out = append(out, tokens[i:j])
	}
	return out
}
