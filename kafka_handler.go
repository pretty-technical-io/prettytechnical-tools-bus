package bus

import (
	"errors"

	"github.com/IBM/sarama"
	"gitlab.com/prettytechnical-tools/logger"
)

var ErrUnknownTopic = errors.New("unknown topic")

type consumerGroupHandler struct {
	handlers map[string]func(msg *sarama.ConsumerMessage) error
	log      logger.Logger
}

func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c consumerGroupHandler) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h, ok := c.handlers[msg.Topic]
		if !ok {
			return ErrUnknownTopic
		}
		if err := h(msg); err != nil {
			c.log.WithError(err).Info("failed to handle kafka message")
		}
	}

	return nil
}

func NewHandler(handlers map[string]func(msg *sarama.ConsumerMessage) error, log logger.Logger) sarama.ConsumerGroupHandler {
	return &consumerGroupHandler{
		log:      log,
		handlers: handlers,
	}
}
