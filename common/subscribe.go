package common

// Subscribe interfaces
type Subscribe interface {
	Put(Event) error
}
