package broker

import (
	"github.com/crizah/Onion/task"
)

type Broker interface {
	Enqueue(ctx context, queue string, task *task.Task) error
}
