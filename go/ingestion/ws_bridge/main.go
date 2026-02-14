package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"trading-platform/go/pkg/shared"

	kc "github.com/zerodha/gokiteconnect/v4"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

// Config specific to ingestion.
type Config struct {
	Kafka        shared.KafkaConfig
	Metrics      shared.MetricsConfig
	TokensCSV    string `envconfig:"ZERODHA_TOKENS_CSV" default:"configs/tokens.csv"`
	TokenJSON    string `envconfig:"ZERODHA_TOKEN_FILE" default:"ingestion/auth/token.json"`
	APIKey       string `envconfig:"KITE_API_KEY"`
	AccessToken  string `envconfig:"KITE_ACCESS_TOKEN"` // optional override
	TickTopic    string `envconfig:"TICKS_TOPIC" default:"ticks"`
	SimTicks     bool   `envconfig:"SIM_TICKS" default:"false"`
	BatchFlushMs int    `envconfig:"BATCH_FLUSH_MS" default:"200"`
	MaxBatch     int    `envconfig:"MAX_BATCH" default:"256"`
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
	t := kc.NewTicker(k.apiKey, k.accessToken)

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
			if err := t.SetMode(kc.ModeLTP, chunk); err != nil {
				k.log.Printf("[ws] set mode failed: %v", err)
			}
		}
	})
	t.OnNoReconnect(func(attempt int) {
		k.log.Printf("[ws] no more reconnects after attempt %d", attempt)
		k.metrics.wsEvents.WithLabelValues("noreconnect").Inc()
	})
	t.OnTicks(func(ticks []kc.Tick) {
		for _, tk := range ticks {
			sym := k.tokenToSym[tk.InstrumentToken]
			if sym == "" {
				continue
			}
			eventTS := tk.Timestamp
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
		}
	})

	go func() {
		<-ctx.Done()
		t.Stop()
	}()

	return t.Serve()
}

// SimSource emits synthetic ticks for testing.
type SimSource struct {
	symbols []string
}

func (s *SimSource) Start(ctx context.Context, out chan<- shared.Tick) error {
	if len(s.symbols) == 0 {
		s.symbols = []string{"SIM"}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				close(out)
				return
			case t := <-ticker.C:
				for _, sym := range s.symbols {
					price := 100 + (float64(t.UnixNano()%1000)-500)/1000.0
					out <- shared.Tick{Symbol: sym, EventTS: t.UnixNano(), LTP: price, Vol: 1}
				}
			}
		}
	}()
	return nil
}

// Metrics bundle.
type ingestMetrics struct {
	ticksOut  *prometheus.CounterVec
	qDepth    prometheus.Gauge
	batchSz   prometheus.Histogram
	latency   prometheus.Histogram
	wsEvents  *prometheus.CounterVec
	dropped   prometheus.Counter
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

func main() {
	var cfg Config
	envconfig.MustProcess("", &cfg)
	logger := shared.NewLogger("ingest")
	metrics := newMetrics()
	ms := shared.NewMetricsServer(cfg.Metrics.Port)
	ms.Start()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, err := shared.NewProducer(cfg.Kafka)
	if err != nil {
		logger.Fatalf("producer init: %v", err)
	}
	defer producer.Close()

	src, err := buildSource(cfg, logger, metrics)
	if err != nil {
		logger.Fatalf("build source: %v", err)
	}

	out := make(chan shared.Tick, 20000)
	if err := src.Start(ctx, out); err != nil {
		logger.Fatalf("source start: %v", err)
	}

	logger.Printf("running ingestion -> topic=%s sim=%v", cfg.TickTopic, cfg.SimTicks)
	batch := make([]shared.Tick, 0, cfg.MaxBatch)
	flushDelay := time.Duration(cfg.BatchFlushMs) * time.Millisecond

	flush := func(batch []shared.Tick) {
		if len(batch) == 0 {
			return
		}
		metrics.batchSz.Observe(float64(len(batch)))
		for _, tk := range batch {
			lat := time.Since(time.Unix(0, tk.EventTS)).Seconds()
			metrics.latency.Observe(lat)
			metrics.ticksOut.WithLabelValues(tk.Symbol).Inc()
			b, _ := json.Marshal(tk)
			_ = producer.Produce(ctx, cfg.TickTopic, []byte(tk.Symbol), b)
		}
	}

	timer := time.NewTimer(flushDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case tk, ok := <-out:
			if !ok {
				return
			}
			batch = append(batch, tk)
			metrics.qDepth.Set(float64(len(out)))
			if len(batch) >= cfg.MaxBatch {
				flush(batch)
				batch = batch[:0]
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(flushDelay)
			}
		case <-timer.C:
			flush(batch)
			batch = batch[:0]
			timer.Reset(flushDelay)
		}
	}
}

func buildSource(cfg Config, logger shared.Logger, m ingestMetrics) (TickSource, error) {
	if cfg.SimTicks {
		syms, err := readSymbols(cfg.TokensCSV)
		if err != nil {
			return nil, err
		}
		return &SimSource{symbols: syms}, nil
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
