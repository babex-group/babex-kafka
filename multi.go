package kafka

import (
	"github.com/babex-group/babex"
	"github.com/bsm/sarama-cluster"
)

func multiListen(adapter *Adapter) {
	defer close(adapter.multi)

	for {
		select {
		case part, ok := <-adapter.Consumer.Partitions():
			if !ok {
				return
			}

			go func(pc cluster.PartitionConsumer) {
				ch := make(chan *babex.Message)

				defer close(ch)

				adapter.multi <- babex.NewChannel(ch)

				for msg := range pc.Messages() {
					m, err := adapter.options.ConvertMessage(adapter.Consumer, msg)
					if err != nil {
						adapter.err <- err
						adapter.Consumer.MarkOffset(msg, "")
						continue
					}

					ch <- m
				}
			}(part)
		}
	}
}
