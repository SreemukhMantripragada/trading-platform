package shared

import (
	"context"
	"encoding/json"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer abstracts Kafka production.
type Producer interface {
	Produce(ctx context.Context, topic string, key []byte, value []byte) error
	Close()
}

// Consumer abstracts Kafka consumption.
type Consumer interface {
	Poll(ctx context.Context) (*kafka.Message, error)
	CommitOffsets(offsets []kafka.TopicPartition) error
	Close()
}

// KafkaProducer implements Producer using confluent-kafka.
type KafkaProducer struct {
	p *kafka.Producer
}

func NewProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
		"linger.ms":        cfg.LingerMS,
		"batch.num.messages": 10000,
		"queue.buffering.max.kbytes": cfg.BatchBytes / 1024,
		"acks":             cfg.ProducerAcks,
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{p: p}, nil
}

func (k *KafkaProducer) Produce(ctx context.Context, topic string, key []byte, value []byte) error {
	deliv := make(chan kafka.Event, 1)
	err := k.p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Key: key, Value: value}, deliv)
	if err != nil {
		return err
	}
	select {
	case ev := <-deliv:
		m := ev.(*kafka.Message)
		return m.TopicPartition.Error
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (k *KafkaProducer) ProduceJSON(ctx context.Context, topic string, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return k.Produce(ctx, topic, key, b)
}

func (k *KafkaProducer) Close() {
	k.p.Flush(3000)
	k.p.Close()
}

// KafkaConsumer implements Consumer using confluent-kafka.
type KafkaConsumer struct {
	c *kafka.Consumer
}

func NewConsumer(cfg KafkaConfig, topics []string) (*KafkaConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
		"group.id":          cfg.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		return nil, err
	}
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return nil, err
	}
	return &KafkaConsumer{c: c}, nil
}

func (k *KafkaConsumer) Poll(ctx context.Context) (*kafka.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		ev := k.c.Poll(200)
		if ev == nil {
			continue
		}
		switch m := ev.(type) {
		case *kafka.Message:
			return m, nil
		case kafka.Error:
			// non-fatal errors
			continue
		default:
			continue
		}
	}
}

func (k *KafkaConsumer) CommitOffsets(offsets []kafka.TopicPartition) error {
	_, err := k.c.CommitOffsets(offsets)
	return err
}

func (k *KafkaConsumer) Close() { _ = k.c.Close() }

// CommitSingle commits the message just processed.
func CommitSingle(c Consumer, msg *kafka.Message) error {
	return c.CommitOffsets([]kafka.TopicPartition{{
		Topic:     msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    msg.TopicPartition.Offset + 1,
	}})
}
