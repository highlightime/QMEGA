package eviction

type Eviction interface {
	SelectVictim() ([]byte, bool)
	Access(key []byte) bool
	Pop() ([]byte, bool)
	Push(key []byte) bool
	Delete(key []byte) bool
}
