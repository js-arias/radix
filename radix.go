// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package radix implement a radix tree. It is expected that the
// keys are in UTF-8 (i.e. go runes), and that insertion and lookup
// is far more common than deletion.
package radix

import (
	"container/list"
	"errors"
	"sync"
)

// Radix is a radix tree.
type Radix struct {
	root *radNode   // root of the radix tree
	lock sync.Mutex // protect the radix
}

// a node of a radix tree
type radNode struct {
	prefix    []rune      // current prefix of the node
	desc, sis *radNode    // neighbors of the node
	value     interface{} // stored value
}

// New returns a new, empty radix tree.
func New() *Radix {
	rad := &Radix{
		root: &radNode{},
	}
	return rad
}

// Delete removes the value associated with a particular key and returns it.
func (rad *Radix) Delete(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	return rad.root.delete([]rune(key))
}

// implements delete
func (r *radNode) delete(key []rune) interface{} {
	if x, ok := r.lookup(key); ok {
		val := x.value
		// only assign a nil, therefore skip any modification
		// of the radix topology
		x.value = nil
		return val
	}
	return nil
}

// Insert put a value in the radix. It returns an error if the given key
// is already in use.
func (rad *Radix) Insert(key string, value interface{}) error {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	return rad.root.insert([]rune(key), value)
}

// implements insert
func (r *radNode) insert(key []rune, value interface{}) error {
	for d := r.desc; d != nil; d = d.sis {
		comm := common(key, d.prefix)
		if len(comm) == 0 {
			continue
		}
		if len(comm) == len(key) {
			if len(comm) == len(d.prefix) {
				if d.value == nil {
					d.value = value
					return nil
				}
				return errors.New("key already in use")
			}
			n := &radNode{
				prefix: make([]rune, len(d.prefix)-len(comm)),
				value:  d.value,
				desc:   d.desc,
			}
			copy(n.prefix, d.prefix[len(comm):])
			d.desc = n
			d.prefix = comm
			d.value = value
			return nil
		}
		if len(comm) == len(d.prefix) {
			return d.insert(key[len(comm):], value)
		}
		p := &radNode{
			prefix: make([]rune, len(d.prefix)-len(comm)),
			value:  d.value,
			desc:   d.desc,
		}
		copy(p.prefix, d.prefix[len(comm):])
		n := &radNode{
			prefix: make([]rune, len(key)-len(comm)),
			value:  value,
		}
		copy(n.prefix, key[len(comm):])
		d.prefix = comm
		p.sis = n
		d.desc = p
		d.value = nil
		return nil
	}
	n := &radNode{
		prefix: make([]rune, len(key)),
		value:  value,
		sis:    r.desc,
	}
	copy(n.prefix, key)
	r.desc = n
	return nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) Lookup(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	if x, ok := rad.root.lookup([]rune(key)); ok {
		return x.value
	}
	return nil
}

// Prefix returns a list of elements that share a given prefix.
func (rad *Radix) Prefix(prefix string) *list.List {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	l := list.New()
	n, _ := rad.root.lookup([]rune(prefix))
	if n == nil {
		return l
	}
	n.addToList(l)
	return l
}

// add the content of a node and its descendants to a list
func (r *radNode) addToList(l *list.List) {
	if r.value != nil {
		l.PushBack(r.value)
	}
	for d := r.desc; d != nil; d = d.sis {
		d.addToList(l)
	}
}

// implementats lookup
func (r *radNode) lookup(key []rune) (*radNode, bool) {
	for d := r.desc; d != nil; d = d.sis {
		comm := common(key, d.prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.prefix) {
				return d, true
			}
			return d, false
		}
		return d.lookup(key[len(comm):])
	}
	return nil, false
}

// return the common string
func common(s, o []rune) []rune {
	max, min := s, o
	if len(max) < len(min) {
		max, min = min, max
	}
	var str []rune
	for i, r := range min {
		if r != max[i] {
			break
		}
		if str == nil {
			str = []rune{r}
		} else {
			str = append(str, r)
		}
	}
	return str
}
