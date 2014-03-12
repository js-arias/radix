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
	prefix         []rune      // current prefix of the node
	desc, sis, par *radNode    // neighbors of the node
	value          interface{} // stored value
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

// BUG(jsa): Insert does not add childs alphabetically

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
				par:    d,
			}
			copy(n.prefix, d.prefix[len(comm):])
			d.desc = n
			n.SetParOnDesc()
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
			par:    d,
		}
		copy(p.prefix, d.prefix[len(comm):])
		n := &radNode{
			prefix: make([]rune, len(key)-len(comm)),
			value:  value,
			par:    d,
		}
		copy(n.prefix, key[len(comm):])
		d.prefix = comm
		p.sis = n
		d.desc = p
		p.SetParOnDesc()
		d.value = nil
		return nil
	}
	n := &radNode{
		prefix: make([]rune, len(key)),
		value:  value,
		sis:    r.desc,
		par:    r,
	}
	copy(n.prefix, key)
	r.desc = n
	return nil
}

// set parent on descendans
func (r *radNode) SetParOnDesc() {
	for x := r.desc; x != nil; x = x.sis {
		x.par = r
	}
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

// Iterator is an iterator of a Radix Tree
type Iterator struct {
	r *radNode

	// Key of the element
	Key string

	// Value assigned to current element
	Value interface{}
}

// NewIterator returns a new iterator for a given Radix tree. If the tree is
// empty, a nil Iterator will be return.
func (rad *Radix) Iterator() *Iterator {
	if rad == nil {
		return nil
	}
	r := rad.root.next()
	if r.value == nil {
		return nil
	}
	it := &Iterator{
		r:     r,
		Key:   string(r.getKey()),
		Value: r.value,
	}
	return it
}

// Next retrieve the next valid iterator, if there are no more elements in the
// radix, a nil iterator will be returned.
func (it *Iterator) Next() *Iterator {
	if it == nil {
		return nil
	}
	r := it.r.next()
	if r == nil {
		return nil
	}
	nx := &Iterator{
		r:     r,
		Key:   r.getKey(),
		Value: r.value,
	}
	return nx
}

// next returns the next valid radix node
func (r *radNode) next() *radNode {

	println("node:", string(r.prefix), "value:", r.value)

	if n := r.getFirst(); n != nil {
		if n.value != nil {
			return n
		}
		return n.next()
	}
	o := r
	for p := r; p != nil; p = p.par {
		if d := o.getNextSis(); d != nil {
			if d.value != nil {
				return d
			}
			return d.next()
		}
		o = p
	}
	return nil
}

// getKey returns the associated key of a radNode
func (r *radNode) getKey() string {
	key := make([]rune, len(r.prefix))
	copy(key, r.prefix)
	for p := r.par; p != nil; p = p.par {
		k := make([]rune, len(p.prefix))
		copy(k, p.prefix)
		k = append(k, key...)
		key = k
	}
	return string(key)
}

// getFirst scans the current radix level to select the first alphabetical
// element.
func (r *radNode) getFirst() *radNode {
	n := r.desc
	if n == nil {
		return nil
	}
	if n.sis == nil {
		return n
	}
	key := string(n.prefix)
	for d := n.sis; d != nil; d = d.sis {
		k := string(d.prefix)
		if k < key {
			n = d
			key = k
		}
	}
	return n
}

// getNextSis scans the current radix level to select the next alphabetical
// element.
func (r *radNode) getNextSis() *radNode {
	if r.par == nil {
		return nil
	}
	minKey := string(r.prefix)
	n := r.par.getLast()
	maxKey := string(n.prefix)
	if n == r {
		return nil
	}
	for d := r.par.desc; d != nil; d = d.sis {
		k := string(d.prefix)
		if k > minKey {
			if k < maxKey {
				n = d
				maxKey = k
			}
		}
	}
	return n
}

// getLast scans the current radix level to select the last alphabetical
// element.
func (r *radNode) getLast() *radNode {
	n := r.desc
	if n == nil {
		return nil
	}
	if n.sis == nil {
		return n
	}
	key := string(n.prefix)
	for d := n.sis; d != nil; d = d.sis {
		k := string(d.prefix)
		if k > key {
			n = d
			key = k
		}
	}
	return n
}
