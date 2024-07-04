package lru

import (
	"testing"
)

func TestPrint(t *testing.T) {
	l := New()
	l.Push([]byte("test"))
}
