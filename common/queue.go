package common

// Queue interfaces
type Queue interface {
	Push(*Event) error
	ID() string
}
