package lru

import (
	"testing"
)

func TestLRU(t *testing.T) {
	l := New()
	if !l.Push([]byte("test1")) {
		t.Error("Error")
	}

	if !l.Push([]byte("test2")) {
		t.Error("Error")
	}

	if !l.Push([]byte("test3")) {
		t.Error("Error")
	}

	if l.Delete([]byte("test4")) {
		t.Error("Error")
	}
	if !l.Delete([]byte("test2")) {
		t.Error("Error")
	}

	key, success := l.SelectVictim()
	if !success {
		t.Error("Error")
	}
	if string(key) != "test3" {
		t.Error("Error")
	}
	if !l.Access([]byte("test1")) {
		t.Error("Error")
	}

	if l.Access([]byte("test4")) {
		t.Error("Error")
	}

	key, success = l.SelectVictim()
	if !success {
		t.Error("Error")
	}
	if string(key) != "test1" {
		t.Error("Error")
	}
	key, success = l.Pop()
	if !success {
		t.Error("Error")
	}

	if string(key) != "test1" {
		t.Error("Error")
	}

	key, success = l.Pop()
	if !success {
		t.Error("Error")
	}

	if string(key) != "test3" {
		t.Error("Error")
	}

	key, success = l.Pop()
	if success {
		t.Error("Error")
	}

	if key != nil {
		t.Error("Error")
	}

	key, success = l.SelectVictim()
	if success {
		t.Error("Error")
	}

	if key != nil {
		t.Error("Error")
	}
}
