package eviction

type Eviction interface {
	SelectVictim() ([]byte, bool)
	Access(key []byte) bool
	Pop() ([]byte, bool, int)
	Len() int
	Push(key []byte, size int) bool
	Delete(key []byte) (bool, int)
}
