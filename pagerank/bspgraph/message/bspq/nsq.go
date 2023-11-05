package bspq

// var _ message.Queue = (*msgQueue)(nil)

// type Config struct {
// 	Address string
// }

// type msgQueue struct {
// 	producer *nsq.Producer
// }

// func New(conf *Config) message.QueueFactory {
// 	prod, err := nsq.NewProducer("string", &nsq.Config{})
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	mq := msgQueue{
// 		producer: prod,
// 	}
// 	return func() message.Queue {
// 		return &mq
// 	}
// }

// // Close implements message.Queue.
// func (mq *msgQueue) Close() error {
// 	mq.producer.Stop()
// 	return nil
// }

// // DiscardMessages implements message.Queue.
// func (*msgQueue) DiscardMessages() error {
// 	panic("unimplemented")
// }

// // Enqueue implements message.Queue.
// func (mq *msgQueue) Enqueue(msg message.Message) error {
// 	return mq.producer.Publish("pagerank", []byte{})
// }

// // Messages implements message.Queue.
// func (*msgQueue) Messages() message.Iterator {
// 	panic("unimplemented")
// }

// // PendingMessages implements message.Queue.
// func (*msgQueue) PendingMessages() bool {
// 	panic("unimplemented")
// }
