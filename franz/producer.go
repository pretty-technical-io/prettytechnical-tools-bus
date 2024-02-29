package franz

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gitlab.com/prettytechnical-tools/logger"
)

var ErrSendingMessage = errors.New("error sending message")

type producer struct {
	opts   []kgo.Opt
	client *kgo.Client
	log    logger.Logger
}

// CreateProducer create and configure a new message sender.
func CreateProducer(brokers, username, password string, retries int, isSSL bool, certsPath string, l logger.Logger) (*producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		// Configure TLS. Uses SystemCertPool for RootCAs by default.
		kgo.RecordRetries(retries),
		kgo.AllowAutoTopicCreation(),
	}

	if isSSL || password != "" {
		tlsDialer, err := getDialerWithSSL(isSSL, certsPath, l)
		if err != nil {
			return nil, err
		}
		// Configure TLS. Uses SystemCertPool for RootCAs by default.
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	// SASL Options.
	if password != "" {
		opts = append(opts, kgo.SASL(scram.Sha512(func(_ context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: username,
				Pass: password,
			}, nil
		})))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		l.Error(err)
		return nil, err
	}

	return &producer{
		opts:   opts,
		client: cl,
		log:    l,
	}, nil
}

// Send is responsible for sending a message.
func (p *producer) Send(ctx context.Context, data []byte, topic, key string, headers map[string][]byte) error {
	if key == "" {
		key = topic
	}

	kafkaHeader := make([]kgo.RecordHeader, 0, len(headers))
	for k, v := range headers {
		kafkaHeader = append(kafkaHeader, kgo.RecordHeader{Key: k, Value: v})
	}

	record := &kgo.Record{Topic: topic, Value: data, Key: []byte(key), Headers: kafkaHeader}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		p.log.Errorf("record had a produce error while synchronously producing: %v\n", err)
		return ErrSendingMessage
	}

	return nil
}

// SendJSON is responsible for marshaling data and sending a message.
func (p *producer) SendJSON(ctx context.Context, data any, topic, key string, headers map[string][]byte) error {
	b, err := json.Marshal(data)
	if err != nil {
		p.log.Error(err)
		return err
	}

	return p.Send(ctx, b, topic, key, headers)
}

// Close the client.
func (p *producer) Close() error {
	if p.client == nil {
		return nil
	}

	p.client.Close()

	return nil
}

// IsHealthy check if the connection with the brokers is alive
func (p *producer) IsHealthy(ctx context.Context) (bool, error) {
	err := p.client.Ping(ctx)
	if err != nil {
		p.log.Error(err)
		return false, err
	}

	return true, nil
}
