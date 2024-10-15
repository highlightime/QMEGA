package lru

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRU(t *testing.T) {
	l := New()
	success := l.Push([]byte("test1"), 1)
	assert.True(t, success)

	success = l.Push([]byte("test2"), 2)
	assert.True(t, success)

	success = l.Push([]byte("test3"), 3)
	assert.True(t, success)

	len := l.Len()
	assert.Equal(t, len, 3)

	success = l.Push([]byte("test3"), 30)
	assert.False(t, success)

	len = l.Len()
	assert.Equal(t, len, 3)

	success, _ = l.Delete([]byte("test4"))
	assert.False(t, success)

	success, size := l.Delete([]byte("test2"))
	assert.True(t, success)
	assert.Equal(t, 2, size)

	len = l.Len()
	assert.Equal(t, len, 2)

	key, success := l.SelectVictim()
	assert.True(t, success)
	assert.Equal(t, string(key), "test1")

	success = l.Access([]byte("test1"))
	assert.True(t, success)

	success = l.Access([]byte("test4"))
	assert.False(t, success)

	key, success = l.SelectVictim()
	assert.True(t, success)
	assert.Equal(t, string(key), "test3")

	key, success, size = l.Pop()
	assert.True(t, success)
	assert.Equal(t, string(key), "test3")
	assert.Equal(t, size, 30)

	key, success, size = l.Pop()
	assert.True(t, success)
	assert.Equal(t, string(key), "test1")
	assert.Equal(t, size, 1)

	key, success, size = l.Pop()
	assert.False(t, success)
	assert.Nil(t, key)
	assert.Equal(t, size, 0)

	key, success = l.SelectVictim()
	assert.False(t, success)
	assert.Nil(t, key)

	len = l.Len()
	assert.Equal(t, len, 0)
}
