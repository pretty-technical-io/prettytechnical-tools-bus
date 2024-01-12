package bus

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"gitlab.com/prettytechnical-tools/logger"
)

type Producer interface {
	Close() error
	IsHealthy() bool
	Send(ctx context.Context, data []byte, topic, key string, partition int) error
	SendJSON(ctx context.Context, data any, topic, key string, partition int) error
}

type ProducerConfig struct {
	KafkaRetries          int
	KafkaBrokers          string
	KafkaUser             string
	KafkaPassword         string
	KafkaClientProducerID string
}

// NewProducer create and configure a new message sender.
func NewProducer(conf *ProducerConfig, l logger.Logger) (Producer, error) {
	pubConf := createPublisherConfigStructProd(conf)
	if pubConf != nil && conf.KafkaRetries > 0 {
		pubConf.Producer.Retry.Max = conf.KafkaRetries
	}

	var addrs []string
	if conf.KafkaBrokers == "" {
		addrs = []string{conf.KafkaBrokers}
	} else {
		addrs = strings.Split(conf.KafkaBrokers, ",")
	}

	client, err := sarama.NewClient(addrs, pubConf)
	if err != nil {
		return nil, err
	}

	sp, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &producer{
		producer: sp,
		client:   client,
		log:      l,
		brokers:  addrs,
	}, nil
}

type producer struct {
	brokers  []string
	producer sarama.SyncProducer
	client   sarama.Client
	log      logger.Logger
}

// Send is responsible for sending a message.
func (m *producer) Send(_ context.Context, data []byte, topic, key string, part int) error {
	if key == "" {
		key = topic
	}

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(part),
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(data),
	}

	partition, offset, err := m.producer.SendMessage(msg)
	if err != nil {
		m.log.Warnf("message is stored in topic (%s)/ partition(%d) / offset(%d): %v \n", topic, partition, offset, err)
		return err
	}

	return nil
}

// SendJSON is responsible for marshaling data and sending a message.
func (m *producer) SendJSON(ctx context.Context, data any, topic, key string, part int) error {
	b, err := json.Marshal(data)
	if err != nil {
		m.log.Error(err)
		return err
	}

	return m.Send(ctx, b, topic, key, part)
}

// Close the client.
func (m producer) Close() error {
	if err := m.producer.Close(); err != nil {
		m.log.Error(err)
		return err
	}

	return nil
}

// IsHealthy check if the connection with the brokers is alive
func (m producer) IsHealthy() bool {
	_, err := m.client.Topics()
	if err != nil {
		m.log.Error(err)
		return false
	}

	return true
}

func createPublisherConfigStructProd(conf *ProducerConfig) *sarama.Config {
	pubConfig := sarama.NewConfig()

	if ku := conf.KafkaUser; ku != "" {
		pubConfig.Net.SASL.Enable = true
		pubConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		pubConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		pubConfig.Net.TLS.Enable = true
		pubConfig.Net.SASL.User = ku
	}
	if kp := conf.KafkaPassword; kp != "" {
		pubConfig.Net.SASL.Password = kp
	}

	pubConfig.Producer.Idempotent = true
	pubConfig.Producer.MaxMessageBytes = 2000000
	pubConfig.Producer.Compression = 3 // CompressionGZIP
	pubConfig.Producer.RequiredAcks = sarama.WaitForAll
	pubConfig.Producer.Timeout = 30 * time.Second
	pubConfig.Producer.Retry.Max = 10
	pubConfig.Producer.Retry.Backoff = 5 * time.Second
	pubConfig.Producer.Return.Successes = true
	pubConfig.Net.WriteTimeout = 40 * time.Second
	pubConfig.Net.DialTimeout = 40 * time.Second
	pubConfig.Net.MaxOpenRequests = 1
	pubConfig.Metadata.Retry.Backoff = 500 * time.Millisecond
	pubConfig.Metadata.RefreshFrequency = 8 * time.Minute
	pubConfig.Producer.Return.Errors = true
	pubConfig.ClientID = conf.KafkaClientProducerID
	sarama.Logger = log.New(os.Stdout, "sarama: ", log.Lshortfile)

	return pubConfig
}
