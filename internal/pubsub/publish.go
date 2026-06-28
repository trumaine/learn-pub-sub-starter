package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	aqmp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *aqmp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, aqmp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}

func PublishGob[T any](ch *aqmp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, aqmp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
}
