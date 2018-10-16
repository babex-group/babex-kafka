package kafka

import "fmt"

func singleListen(adapter *Adapter) {
MainLoop:
	for {
		select {
		case msg, ok := <-adapter.Consumer.Messages():
			if !ok {
				break MainLoop
			}

			m, err := NewMessage(adapter.Consumer, msg)
			if err != nil {
				adapter.err <- err
				adapter.Consumer.MarkOffset(msg, "")
				continue
			}

			adapter.ch <- m
		case err, ok := <-adapter.Consumer.Errors():
			if ok {
				adapter.err <- fmt.Errorf("kafka consumer error: %s", err)
			}
		}
	}

	close(adapter.ch)
}
