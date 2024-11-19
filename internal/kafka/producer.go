package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
	logger   *zap.Logger
}

func NewProducer(config map[string]string, topic string, logger *zap.Logger) (*Producer, error) {
	// Log the incoming configuration
	logger.Info("creating kafka producer",
		zap.String("bootstrap_servers", config["bootstrap.servers"]),
		zap.String("security_protocol", config["security.protocol"]),
		zap.String("sasl_mechanisms", config["sasl.mechanisms"]),
		zap.String("topic", topic))

	// Create ConfigMap properly
	kafkaConfig := &kafka.ConfigMap{}

	// Set required configurations
	err := kafkaConfig.SetKey("bootstrap.servers", config["bootstrap.servers"])
	if err != nil {
		return nil, fmt.Errorf("failed to set bootstrap.servers: %v", err)
	}

	err = kafkaConfig.SetKey("security.protocol", config["security.protocol"])
	if err != nil {
		return nil, fmt.Errorf("failed to set security.protocol: %v", err)
	}

	err = kafkaConfig.SetKey("sasl.mechanisms", config["sasl.mechanisms"])
	if err != nil {
		return nil, fmt.Errorf("failed to set sasl.mechanisms: %v", err)
	}

	err = kafkaConfig.SetKey("sasl.username", config["sasl.username"])
	if err != nil {
		return nil, fmt.Errorf("failed to set sasl.username: %v", err)
	}

	err = kafkaConfig.SetKey("sasl.password", config["sasl.password"])
	if err != nil {
		return nil, fmt.Errorf("failed to set sasl.password: %v", err)
	}

	// Set additional configurations
	_ = kafkaConfig.SetKey("go.delivery.reports", true)
	_ = kafkaConfig.SetKey("go.events.channel.size", 10000)
	_ = kafkaConfig.SetKey("message.timeout.ms", 5000)
	_ = kafkaConfig.SetKey("retries", 3)

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %v", err)
	}

	// Start a goroutine to handle delivery reports
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logger.Error("failed to deliver message",
						zap.Error(ev.TopicPartition.Error),
						zap.String("topic", *ev.TopicPartition.Topic))
				} else {
					logger.Debug("message delivered",
						zap.String("topic", *ev.TopicPartition.Topic),
						zap.Int32("partition", ev.TopicPartition.Partition),
						zap.Int64("offset", int64(ev.TopicPartition.Offset)))
				}
			}
		}
	}()

	return &Producer{
		producer: producer,
		topic:    topic,
		logger:   logger,
	}, nil
}
func (p *Producer) Produce(message []byte) error {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}

	return nil
}

func (p *Producer) Close() error {
	p.producer.Flush(15 * 1000)
	p.producer.Close()
	return nil
}
