package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	aqmp "github.com/rabbitmq/amqp091-go"
)

func publish[T any](ch *aqmp.Channel, exchange, key string, val T, marshaller func(T) (aqmp.Publishing, error)) error {
	data, err := marshaller(val)
	if err != nil {
		return fmt.Errorf("failed to serialize payload: %v", err)
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, data)
}

func PublishJSON[T any](ch *aqmp.Channel, exchange, key string, val T) error {
	return publish(
		ch,
		exchange,
		key,
		val,
		func(val T) (aqmp.Publishing, error) {
			data, err := json.Marshal(val)
			if err != nil {
				return aqmp.Publishing{}, err
			}
			return aqmp.Publishing{ContentType: "application/json", Body: data}, nil
		},
	)
}

func PublishGob[T any](ch *aqmp.Channel, exchange, key string, val T) error {
	return publish(
		ch,
		exchange,
		key,
		val,
		func(val T) (aqmp.Publishing, error) {
			var buffer bytes.Buffer
			encoder := gob.NewEncoder(&buffer)
			err := encoder.Encode(val)
			if err != nil {
				return aqmp.Publishing{}, err
			}
			return aqmp.Publishing{ContentType: "application/gob", Body: buffer.Bytes()}, nil
		},
	)
}
