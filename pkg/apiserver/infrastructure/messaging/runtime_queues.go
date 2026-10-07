package messaging

// RuntimeQueues groups the cross-node dispatch and delay notification queues.
// Every node initializes both queues so it can become Leader or Worker.
type RuntimeQueues struct {
	Dispatch Queue
	Delay    Queue
}
