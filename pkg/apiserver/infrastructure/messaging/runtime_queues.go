package messaging

// RuntimeQueues groups the Redis Streams queues every node initializes,
// because any node can become Leader.
type RuntimeQueues struct {
	Dispatch Queue
	Delay    Queue
	Result   Queue
}
