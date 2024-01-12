package bus

import (
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

func createConsumerConfigStructProd(user, password, clientID string) *sarama.Config {
	// RETURNS THE SANE DEFAULT CONFIG STRUCT
	config := sarama.NewConfig()

	if user != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
		config.Net.TLS.Enable = true
		config.Net.SASL.User = user
	}
	if password != "" {
		config.Net.SASL.Password = password
	}

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Return.Errors = true

	// PROD PRODUCER
	config.Net.DialTimeout = 40 * time.Second
	config.Net.ReadTimeout = 40 * time.Second

	// How many outstanding requests a connection is allowed to have before
	// sending on it blocks (default 5).
	config.Net.MaxOpenRequests = 1
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Metadata.RefreshFrequency = 8 * time.Minute

	if clientID == "" {
		clientID = "mikado-notification-consumer"
	}
	config.ClientID = clientID
	sarama.Logger = log.New(os.Stdout, "sarama: ", log.Lshortfile)

	return config
}

// ClientConfig contains the kafka configuration variables.
type ClientConfig struct {
	User     string
	Password string
	ClientID string
}
