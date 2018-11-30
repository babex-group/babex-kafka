package kafka

import (
	"fmt"
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

				adapter.logger.Log(fmt.Sprintf("debug: get partition %v for %s", pc.Partition(), pc.Topic()))

				adapter.multi <- babex.NewChannel(ch)

				for msg := range pc.Messages() {
					adapter.logger.Log(
						fmt.Sprintf(
							"debug: get message in %v for %s. offset - %v",
							pc.Partition(),
							pc.Topic(),
							msg.Offset,
						),
					)

					m, err := adapter.options.ConvertMessage(adapter.Consumer, msg)
					if err != nil {
						adapter.err <- err
						adapter.Consumer.MarkOffset(msg, "")
						continue
					}

					ch <- m

					adapter.logger.Log(
						fmt.Sprintf(
							"debug: success publish in %v for %s. offset - %v",
							pc.Partition(),
							pc.Topic(),
							msg.Offset,
						),
					)
				}
			}(part)
		}
	}
}
