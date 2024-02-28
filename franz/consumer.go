package franz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gitlab.com/prettytechnical-tools/logger"
)

var (
	ErrCertificates        = fmt.Errorf("error loading certificates")
	ErrCertificateNotFound = fmt.Errorf("error certificate not found")
	ErrUserCertificate     = fmt.Errorf("error loading user certificate")
)

type consumerGroup struct {
	opts   []kgo.Opt
	client *kgo.Client
	log    logger.Logger
}

// CreateConsumerGroup create and configure a new consumer group.
func CreateConsumerGroup(brokers, cg, username, password string, isSSL bool, certsPath string, isOldest bool, l logger.Logger) (*consumerGroup, error) {
	tlsDialer, err := getDialerWithSSL(isSSL, certsPath, l)
	if err != nil {
		return nil, err
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumerGroup(cg),
		// Configure TLS. Uses SystemCertPool for RootCAs by default.
		kgo.Dialer(tlsDialer.DialContext),
	}

	// SASL Options.
	if password != "" {
		opts = append(opts, kgo.SASL(scram.Sha512(func(ctx context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: username,
				Pass: password,
			}, nil
		})))
	}

	if isOldest {
		// earliest.
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	return &consumerGroup{
		opts: opts,
		log:  l,
	}, nil
}

// Consume messages from the given topics.
func (c *consumerGroup) Consume(ctx context.Context, maxBatchSize int, handlers map[string]func(key string, msg []byte,
	headers map[string][]byte, partition, offset int) error) error {
	sigchan := make(chan os.Signal, 1)

	semaphore := make(chan struct{}, maxBatchSize)

	topics := make([]string, 0, len(handlers))
	for topic := range handlers {
		topics = append(topics, topic)
	}

	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c.opts = append(c.opts, kgo.ConsumeTopics(topics...))

	cl, err := kgo.NewClient(c.opts...)
	if err != nil {
		return err
	}

	c.client = cl

	defer cl.Close()

	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			return nil
		case <-ctx.Done():
			c.log.Info("Context canceled")
			return nil
		default:
			fetches := cl.PollRecords(ctx, maxBatchSize)
			if fetches.IsClientClosed() {
				c.log.Info("client closed")
				return nil
			}
			fetches.EachError(func(t string, p int32, err error) {
				c.log.Errorf("fetch err topic %s partition %d: %v", t, p, err)
			})

			var rs []*kgo.Record
			fetches.EachRecord(func(r *kgo.Record) {
				rs = append(rs, r)
			})
			if err := cl.CommitRecords(context.Background(), rs...); err != nil {
				fmt.Printf("commit records failed: %v", err)
				continue
			}

			c.log.Infof("Received [%d] messages\n", len(rs))
			for _, m := range rs {
				semaphore <- struct{}{}
				go func(record *kgo.Record) {
					var headers map[string][]byte
					if record.Headers != nil {
						headers = make(map[string][]byte, len(record.Headers))
						for _, h := range record.Headers {
							headers[h.Key] = h.Value
						}
					}

					if f, ok := handlers[record.Topic]; ok {
						err := f(string(record.Key), record.Value, headers, int(record.Partition), int(record.Offset))
						if err != nil {
							c.log.Errorf("Error processing message: %v\n", err)
							return
						}
					}
					<-semaphore
				}(m)
			}
		}
	}
}

// Close the client.
func (c *consumerGroup) Close() error {
	if c.client == nil {
		return nil
	}

	c.client.Close()

	return nil
}

func getDialerWithSSL(isSSL bool, certsPath string, l logger.Logger) (*tls.Dialer, error) {
	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 30 * time.Second}, Config: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec

	if isSSL {
		caCertPath := certsPath + "/ca.crt"
		userCertPath := certsPath + "/user.crt"
		userKeyPath := certsPath + "/user.key"

		caCert, err := os.ReadFile(caCertPath)
		if err != nil {
			l.Error(err)
			return nil, ErrCertificateNotFound
		}

		userCert, err := tls.LoadX509KeyPair(userCertPath, userKeyPath)
		if err != nil {
			l.Error(err)
			return nil, ErrUserCertificate
		}

		rootCA := x509.NewCertPool()
		if ok := rootCA.AppendCertsFromPEM(caCert); !ok {
			return nil, ErrCertificates
		}

		tlsDialer.Config = &tls.Config{
			RootCAs:            rootCA,
			Certificates:       []tls.Certificate{userCert},
			InsecureSkipVerify: true, //nolint:gosec
		}
	}

	return tlsDialer, nil
}
