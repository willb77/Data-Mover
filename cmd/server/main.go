package main

import (
	"context"
	"data-mover/internal/config"
	"data-mover/internal/kafka"
	"data-mover/internal/pubsub"
	"data-mover/internal/service"
	"flag"
	"go.uber.org/zap"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "config/config.yaml", "path to config file")
	flag.Parse()

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	defer func() {
		if err := logger.Sync(); err != nil {
			log.Printf("failed to sync logger: %v", err)
		}
	}()

	// Load configuration with logging
	logger.Info("loading configuration", zap.String("config_path", *configPath))
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Pub/Sub subscriber
	subscriber, err := pubsub.NewSubscriber(ctx, cfg.PubSub.ProjectID, cfg.PubSub.SubscriptionID, logger)
	if err != nil {
		logger.Fatal("failed to create subscriber", zap.Error(err))
	}
	defer subscriber.Close()

	// Initialize Kafka producer
	kafkaConfig := map[string]string{
		"bootstrap.servers": cfg.Kafka.BootstrapServers,
		"security.protocol": cfg.Kafka.SecurityProtocol,
		"sasl.mechanisms":   cfg.Kafka.SASLMechanisms,
		"sasl.username":     cfg.Kafka.SASLUsername,
		"sasl.password":     cfg.Kafka.SASLPassword,
	}

	// Add debug logging
	logger.Info("creating kafka producer with config",
		zap.String("bootstrap_servers", kafkaConfig["bootstrap.servers"]),
		zap.String("security_protocol", kafkaConfig["security.protocol"]),
		zap.String("sasl_mechanisms", kafkaConfig["sasl.mechanisms"]),
		zap.String("topic", cfg.Kafka.Topic))

	producer, err := kafka.NewProducer(kafkaConfig, cfg.Kafka.Topic, logger)
	if err != nil {
		logger.Fatal("failed to create producer", zap.Error(err))
	}

	// Initialize the bridge service
	bridge := service.NewBridge(subscriber, producer, logger)

	// Handle shutdown gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the bridge service
	go func() {
		if err := bridge.Start(ctx); err != nil {
			logger.Error("bridge service error", zap.Error(err))
			cancel()
		}
	}()

	// Start metrics reporting
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				metrics := bridge.GetMetrics()
				logger.Info("service metrics",
					zap.Int64("messages_processed", metrics.MessagesProcessed),
					zap.Int64("messages_failed_pubsub", metrics.MessagesFailedPubSub),
					zap.Int64("messages_failed_kafka", metrics.MessagesFailedKafka),
					zap.Time("last_processed_time", metrics.LastProcessedTime))
			}
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("shutting down service...")

	// Give outstanding operations up to 30 seconds to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := bridge.Stop(); err != nil {
		logger.Error("error during shutdown", zap.Error(err))
	}

	select {
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timed out")
	default:
		logger.Info("shutdown completed successfully")
	}
}
