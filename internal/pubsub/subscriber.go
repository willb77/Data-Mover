package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"go.uber.org/zap"
)

type Subscriber struct {
	client       *pubsub.Client
	subscription *pubsub.Subscription
	logger       *zap.Logger
}

type MessageHandler func(ctx context.Context, data []byte) error

func NewSubscriber(ctx context.Context, projectID, subscriptionID string, logger *zap.Logger) (*Subscriber, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		if cerr := client.Close(); cerr != nil {
			logger.Error("failed to close client after error", zap.Error(cerr))
		}
		return nil, fmt.Errorf("failed to check subscription existence: %v", err)
	}
	subscription := client.Subscription(subscriptionID)
	exists, err := subscription.Exists(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to check subscription existence: %v", err)
	}
	if !exists {
		return nil, fmt.Errorf("subscription %s does not exist", subscriptionID)
	}

	// Configure subscription to retry messages on error
	subscription.ReceiveSettings.MaxOutstandingMessages = 100
	subscription.ReceiveSettings.MaxExtension = pubsub.DefaultReceiveSettings.MaxExtension

	return &Subscriber{
		client:       client,
		subscription: subscription,
		logger:       logger,
	}, nil
}

func (s *Subscriber) Start(ctx context.Context, handler MessageHandler) error {
	return s.subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		err := handler(ctx, msg.Data)
		if err != nil {
			s.logger.Error("failed to process message",
				zap.Error(err),
				zap.String("message_id", msg.ID))
			msg.Nack()
			return
		}

		msg.Ack()
		s.logger.Debug("message processed successfully",
			zap.String("message_id", msg.ID))
	})
}

func (s *Subscriber) Close() error {
	if err := s.client.Close(); err != nil {
		return fmt.Errorf("failed to close pubsub client: %v", err)
	}
	return nil
}
