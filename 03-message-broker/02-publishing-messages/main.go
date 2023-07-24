package main

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"os"
)

type ZapLoggerAdapter struct {
	logger *zap.Logger
}

func (z *ZapLoggerAdapter) Error(msg string, err error, fields watermill.LogFields) {
	z.logger.Error(msg, zap.Error(err))
}

func (z *ZapLoggerAdapter) Info(msg string, fields watermill.LogFields) {
	zFields := make([]zap.Field, 0)
	for fieldName, fieldValue := range fields {
		zFields = append(zFields, zap.String(fieldName, fmt.Sprint(fieldValue)))
	}

	z.logger.Info(msg, zFields...)
}
func (z *ZapLoggerAdapter) Debug(msg string, fields watermill.LogFields) {}
func (z *ZapLoggerAdapter) Trace(msg string, fields watermill.LogFields) {}
func (z *ZapLoggerAdapter) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &ZapLoggerAdapter{logger: z.logger}
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	loggerAdapter := ZapLoggerAdapter{logger: logger}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, &loggerAdapter)
	if err != nil {
		panic(err)
	}

	publisher.Publish("progress", &message.Message{
		UUID:    watermill.NewUUID(),
		Payload: []byte("50"),
	})

	publisher.Publish("progress", &message.Message{
		UUID:    watermill.NewUUID(),
		Payload: []byte("100"),
	})
}
