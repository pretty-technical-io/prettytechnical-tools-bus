package bus

import (
	"context"
	"strings"

	"github.com/IBM/sarama"
	"gitlab.com/prettytechnical-tools/logger"
)

type Client interface {
	Close() error
	IsHealthy() bool
	Start(ctx context.Context, topic []string, handler sarama.ConsumerGroupHandler) error
}

type message struct {
	brokers  []string
	consumer sarama.ConsumerGroup
	client   sarama.Client
	log      logger.Logger
}

// Start is responsible for reading messages.
func (m *message) Start(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	// Track errors.
	go func() {
		for err := range m.consumer.Errors() {
			m.log.Error(err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			err := m.consumer.Consume(ctx, topics, handler)
			if err != nil {
				m.log.Error(err)
			}
		}
	}
}

// Close the client.
func (m *message) Close() error {
	if err := m.consumer.Close(); err != nil {
		m.log.Error(err)
		return err
	}

	if err := m.client.Close(); err != nil {
		m.log.Error(err)
		return err
	}

	return nil
}

// IsHealthy check if the connection with the brokers is alive.
func (m *message) IsHealthy() bool {
	_, err := m.client.Topics()
	if err != nil {
		m.log.Error(err)
		return false
	}

	return true
}

// New create and configure a new message sender.
func New(brokers, groupID string, conf ClientConfig, log logger.Logger) (Client, error) {
	config := createConsumerConfigStructProd(conf.User, conf.Password, conf.ClientID)
	var addrs []string
	if brokers == "" {
		addrs = []string{brokers}
	} else {
		addrs = strings.Split(brokers, ",")
	}

	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}

	return &message{
		consumer: consumerGroup,
		client:   client,
		log:      log,
		brokers:  addrs,
	}, nil
}
