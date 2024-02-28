package kafka

import (
	"context"
)

type Producer interface {
	Close() error
	IsHealthy(ctx context.Context) (bool, error)
	Send(ctx context.Context, data []byte, topic, key string, headers map[string][]byte) error
	SendJSON(ctx context.Context, data any, topic, key string, headers map[string][]byte) error
}
