package service

import (
	"context"
	"data-mover/internal/kafka"
	"data-mover/internal/pubsub"
	"fmt"
	"go.uber.org/zap"
	"time"
)

type Bridge struct {
	subscriber *pubsub.Subscriber
	producer   *kafka.Producer
	logger     *zap.Logger
	metrics    *Metrics
}

type Metrics struct {
	MessagesProcessed    int64
	MessagesFailedPubSub int64
	MessagesFailedKafka  int64
	LastProcessedTime    time.Time
}

func NewBridge(subscriber *pubsub.Subscriber, producer *kafka.Producer, logger *zap.Logger) *Bridge {
	return &Bridge{
		subscriber: subscriber,
		producer:   producer,
		logger:     logger,
		metrics:    &Metrics{},
	}
}

func (b *Bridge) Start(ctx context.Context) error {
	b.logger.Info("starting pubsub-kafka bridge service")

	return b.subscriber.Start(ctx, func(ctx context.Context, data []byte) error {
		// Record processing time
		startTime := time.Now()

		// Produce to Kafka
		if err := b.producer.Produce(data); err != nil {
			b.metrics.MessagesFailedKafka++
			b.logger.Error("failed to produce to kafka",
				zap.Error(err),
				zap.Int64("failed_count", b.metrics.MessagesFailedKafka))
			return fmt.Errorf("kafka produce error: %v", err)
		}

		if err != nil {
			b.metrics.MessagesFailedKafka++
			b.logger.Error("failed to produce to kafka",
				zap.Error(err),
				zap.Int64("failed_count", b.metrics.MessagesFailedKafka))
			return err
		}

		// Update metrics
		b.metrics.MessagesProcessed++
		b.metrics.LastProcessedTime = time.Now()

		b.logger.Debug("message bridged successfully",
			zap.Int64("total_processed", b.metrics.MessagesProcessed),
			zap.Duration("processing_time", time.Since(startTime)))

		return nil
	})
}

func (b *Bridge) Stop() {
	b.logger.Info("stopping bridge service",
		zap.Int64("total_processed", b.metrics.MessagesProcessed),
		zap.Int64("failed_pubsub", b.metrics.MessagesFailedPubSub),
		zap.Int64("failed_kafka", b.metrics.MessagesFailedKafka))

	b.producer.Close()
	if err := b.subscriber.Close(); err != nil {
		b.logger.Error("error closing subscriber", zap.Error(err))
	}
}

func (b *Bridge) GetMetrics() *Metrics {
	return b.metrics
}
