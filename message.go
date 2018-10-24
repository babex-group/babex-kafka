package kafka

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/babex-group/babex"
	"github.com/bsm/sarama-cluster"
)

type Converter func(consumer *cluster.Consumer, msg *sarama.ConsumerMessage) (*babex.Message, error)

func NewMessage(consumer *cluster.Consumer, msg *sarama.ConsumerMessage) (*babex.Message, error) {
	var initialMessage babex.InitialMessage

	if err := json.Unmarshal(msg.Value, &initialMessage); err != nil {
		return nil, err
	}

	message := babex.NewMessage(&initialMessage, msg.Topic, string(msg.Key))
	message.RawMessage = Message{Msg: msg, consumer: consumer}

	return message, nil
}

type Message struct {
	Msg      *sarama.ConsumerMessage
	consumer *cluster.Consumer
}

func (m Message) Ack() error {
	m.consumer.MarkOffset(m.Msg, "")
	return nil
}

func (m Message) Nack() error {
	return nil
}
