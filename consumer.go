package kafka

import "context"

type Consumer interface {
	Close() error
	Consume(ctx context.Context, maxBatchSize int, handlers map[string]func(key string, msg []byte,
		headers map[string][]byte, partition, offset int) error) error
}
