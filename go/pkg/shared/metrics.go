package shared

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer exposes Prometheus metrics.
type MetricsServer struct {
	addr string
}

func NewMetricsServer(port int) *MetricsServer {
	return &MetricsServer{addr: ":" + itoa(port)}
}

func (m *MetricsServer) Start() {
	go func() { _ = http.ListenAndServe(m.addr, promhttp.Handler()) }()
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// Convenience helpers to avoid repeating namespace.
func NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	prometheus.MustRegister(c)
	return c
}

func NewCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	prometheus.MustRegister(c)
	return c
}

func NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	prometheus.MustRegister(g)
	return g
}

func NewGaugeVec(opts prometheus.GaugeOpts, labels []string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(opts, labels)
	prometheus.MustRegister(g)
	return g
}

func NewHist(opts prometheus.HistogramOpts) prometheus.Histogram {
	h := prometheus.NewHistogram(opts)
	prometheus.MustRegister(h)
	return h
}

func NewHistVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	prometheus.MustRegister(h)
	return h
}
