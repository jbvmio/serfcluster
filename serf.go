package main

import (
	"sync"
)

// MembersToNotify .
const MembersToNotify = 2

type oneAndOnlyNumber struct {
	num        int
	generation int
	meta       string
	numMutex   sync.RWMutex
}

// InitTheNumber .
func InitTheNumber(val int) *oneAndOnlyNumber {
	return &oneAndOnlyNumber{
		num: val,
	}
}

func (n *oneAndOnlyNumber) setValue(newVal int, meta string) {
	n.numMutex.Lock()
	defer n.numMutex.Unlock()
	n.num = newVal
	n.meta = meta
	n.generation = n.generation + 1
}

func (n *oneAndOnlyNumber) getValue() (int, int, string) {
	n.numMutex.RLock()
	defer n.numMutex.RUnlock()
	return n.num, n.generation, n.meta
}

func (n *oneAndOnlyNumber) notifyValue(curVal int, curGeneration int, meta string) bool {
	if curGeneration > n.generation {
		n.numMutex.Lock()
		defer n.numMutex.Unlock()
		n.generation = curGeneration
		n.num = curVal
		n.meta = meta
		return true
	}
	return false
}
