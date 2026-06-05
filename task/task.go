package task

type State string

const (
	RUNNING   State = "running"
	FAILED    State = "failed"
	PENDING   State = "pending"
	COMPLETED State = "completed"
)

type Task struct {
	Id    string         `json:"id"`
	Name  string         `json:"string"`
	Args  map[string]any `json:"args"`
	State State          `json:"state"`
}
