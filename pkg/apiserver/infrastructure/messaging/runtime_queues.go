package messaging

// RuntimeQueues groups the queue clients initialized for one runtime role.
// A nil field means that the role neither produces to nor consumes from that
// queue and therefore must not depend on its availability.
type RuntimeQueues struct {
	Dispatch Queue
	Delay    Queue
	Result   Queue
}
