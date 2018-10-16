# Babex-kafka

Kafka adapter for babex


## Examples

Adapter has two mode of consuming.
In the default mode messages of multiple topics and partitions are all passed to the single channel:

```go
package main

import (
    "github.com/babex-group/babex-kafka"
)

func main() {
    adapter, err := kafka.NewAdapter(kafka.Options{
        Name:   "collector",
        Addrs:  cfg.Brokers,
        Topics: cfg.Topics,
    })
    if err != nil {
        return nil, err
    }

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    s := babex.NewService(adapter)

    msgs, _ := s.GetMessages()

    defer s.Close()

    for {
        select {
        case msg, ok := <- msgs:
            if !ok {
                return
            }

            msg.Ack()
        case <-signals:
            return
        }
    }
}

```


Also you can use multi mode:

```go
package main

import (
    "github.com/babex-group/babex-kafka"
)

func main() {
    adapter, err := kafka.NewAdapter(kafka.Options{
        Name:   "collector",
        Addrs:  cfg.Brokers,
        Topics: cfg.Topics,
        Mode:   kafka.ModeMulti
    })
    if err != nil {
        return nil, err
    }

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    s := babex.NewService(adapter)

    defer s.Close()

    msgs, _ := s.GetMessages()

    for {
        select {
        case ch, ok := <- s.GetChannels():
            if !ok {
                return
            }

            go func(ch *babex.Channel) {
                for msg := range ch.GetMessages() {
                    msg.Ack()
                }
            }(ch)
        case <- signals:
            return
        }
    }
}
```