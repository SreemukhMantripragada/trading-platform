package shared

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Message is the internal broker message shape used by services.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}

// Record is the producer payload shape for batched writes.
type Record struct {
	Key   []byte
	Value []byte
	Time  time.Time
}

// Producer abstracts Kafka production.
type Producer interface {
	Produce(ctx context.Context, topic string, key []byte, value []byte) error
	ProduceBatch(ctx context.Context, topic string, records []Record) error
	ProduceJSON(ctx context.Context, topic string, key []byte, v any) error
	Close()
}

// Consumer abstracts Kafka consumption.
type Consumer interface {
	Poll(ctx context.Context) (*Message, error)
	Commit(msg *Message) error
	Close()
}

// KafkaProducer implements Producer using segmentio/kafka-go.
type KafkaProducer struct {
	cfg     KafkaConfig
	mu      sync.Mutex
	writers map[string]*kafka.Writer
}

func NewProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	return &KafkaProducer{
		cfg:     cfg,
		writers: make(map[string]*kafka.Writer),
	}, nil
}

func (k *KafkaProducer) writer(topic string) *kafka.Writer {
	k.mu.Lock()
	defer k.mu.Unlock()
	if w, ok := k.writers[topic]; ok {
		return w
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.cfg.BrokerList()...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: writerAcks(k.cfg.ProducerAcks),
		BatchTimeout: time.Duration(maxInt(k.cfg.LingerMS, 0)) * time.Millisecond,
		BatchBytes:   int64(maxInt(k.cfg.BatchBytes, 1)),
	}
	k.writers[topic] = w
	return w
}

func (k *KafkaProducer) Produce(ctx context.Context, topic string, key []byte, value []byte) error {
	return k.ProduceBatch(ctx, topic, []Record{{Key: key, Value: value, Time: time.Now().UTC()}})
}

func (k *KafkaProducer) ProduceBatch(ctx context.Context, topic string, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(records))
	now := time.Now().UTC()
	for _, rec := range records {
		msgTime := rec.Time
		if msgTime.IsZero() {
			msgTime = now
		}
		msgs = append(msgs, kafka.Message{
			Key:   rec.Key,
			Value: rec.Value,
			Time:  msgTime,
		})
	}
	return k.writer(topic).WriteMessages(ctx, msgs...)
}

func (k *KafkaProducer) ProduceJSON(ctx context.Context, topic string, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return k.Produce(ctx, topic, key, b)
}

func (k *KafkaProducer) Close() {
	k.mu.Lock()
	ws := make([]*kafka.Writer, 0, len(k.writers))
	for _, w := range k.writers {
		ws = append(ws, w)
	}
	k.writers = make(map[string]*kafka.Writer)
	k.mu.Unlock()
	for _, w := range ws {
		_ = w.Close()
	}
}

// KafkaConsumer implements Consumer using segmentio/kafka-go.
type KafkaConsumer struct {
	r *kafka.Reader
}

func NewConsumer(cfg KafkaConfig, topics []string) (*KafkaConsumer, error) {
	if len(topics) == 0 {
		return nil, errors.New("at least one topic required")
	}
	readerCfg := kafka.ReaderConfig{
		Brokers:        cfg.BrokerList(),
		GroupID:        cfg.GroupID,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	}
	if len(topics) == 1 {
		readerCfg.Topic = topics[0]
	} else {
		readerCfg.GroupTopics = topics
	}
	return &KafkaConsumer{r: kafka.NewReader(readerCfg)}, nil
}

func (k *KafkaConsumer) Poll(ctx context.Context) (*Message, error) {
	msg, err := k.r.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	return &Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Time:      msg.Time,
	}, nil
}

func (k *KafkaConsumer) Commit(msg *Message) error {
	if msg == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return k.r.CommitMessages(ctx, kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
}

func (k *KafkaConsumer) Close() { _ = k.r.Close() }

// CommitSingle commits the message just processed.
func CommitSingle(c Consumer, msg *Message) error {
	return c.Commit(msg)
}

func writerAcks(raw string) kafka.RequiredAcks {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "all", "-1":
		return kafka.RequireAll
	case "none", "0":
		return kafka.RequireNone
	default:
		return kafka.RequireOne
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
