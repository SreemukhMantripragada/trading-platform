package shared

import (
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaConfig holds broker and topic details.
type KafkaConfig struct {
	Brokers      string `envconfig:"KAFKA_BROKER" default:"localhost:9092"`
	GroupID      string `envconfig:"KAFKA_GROUP" default:"go-default-group"`
	InTopic      string `envconfig:"IN_TOPIC"`
	OutTopic     string `envconfig:"OUT_TOPIC"`
	ProducerAcks string `envconfig:"KAFKA_ACKS" default:"all"`
	LingerMS     int    `envconfig:"KAFKA_LINGER_MS" default:"5"`
	BatchBytes   int    `envconfig:"KAFKA_BATCH_BYTES" default:"1048576"` // 1MB
}

func (k KafkaConfig) BrokerList() []string {
	parts := strings.Split(k.Brokers, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

// PostgresConfig holds DB connection details.
type PostgresConfig struct {
	Host     string `envconfig:"POSTGRES_HOST" default:"localhost"`
	Port     int    `envconfig:"POSTGRES_PORT" default:"5432"`
	Database string `envconfig:"POSTGRES_DB" default:"trading"`
	User     string `envconfig:"POSTGRES_USER" default:"trader"`
	Password string `envconfig:"POSTGRES_PASSWORD" default:"trader"`
	PoolMax  int    `envconfig:"PG_POOL_MAX" default:"8"`
}

// MetricsConfig controls Prometheus listener.
type MetricsConfig struct {
	Port int `envconfig:"METRICS_PORT" default:"9000"`
}

// GraceConfig holds timing knobs.
type GraceConfig struct {
	FlushGrace time.Duration `envconfig:"FLUSH_GRACE_SEC" default:"2s"`
	BatchSize  int           `envconfig:"BATCH_SIZE" default:"2000"`
}

// Load fills the given struct from environment.
func Load[T any](prefix string) (T, error) {
	var cfg T
	err := envconfig.Process(prefix, &cfg)
	return cfg, err
}
