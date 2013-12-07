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
	"strings"
	"sync"
)

// Radix is a radix tree.
type Radix struct {
	Root *radNode   // Root of the radix tree
	lock sync.Mutex // protect the radix
}

// a node of a radix tree
type radNode struct {
	Prefix   string      // current prefix of the node
	Children []*radNode  // neighbors of the node
	Value    interface{} // stored Value
}

// New returns a new, empty radix tree.
func New() *Radix {
	rad := &Radix{
		Root: &radNode{},
	}
	return rad
}

// Delete removes the Value associated with a particular key and returns it.
func (rad *Radix) Delete(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	return rad.Root.delete(key)
}

// implements delete
func (r *radNode) delete(key string) interface{} {
	if x, ok := r.lookup(key); ok {
		val := x.Value
		// only assign a nil, therefore skip any modification
		// of the radix topology
		x.Value = nil
		return val
	}
	return nil
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
func (rad *Radix) Insert(key string, Value interface{}) error {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	return rad.Root.insert(key, Value)
}

// implements insert
func (r *radNode) insert(key string, Value interface{}) error {
	for _, d := range r.Children {
		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				if d.Value == nil {
					d.Value = Value
					return nil
				}
				return errors.New("key already in use")
			}
			n := &radNode{
				Prefix:   d.Prefix[len(comm):],
				Value:    d.Value,
				Children: d.Children,
			}
			d.Children = make([]*radNode, 0)
			d.Children = append(d.Children, n)
			d.Prefix = comm
			d.Value = Value
			return nil
		}
		if len(comm) == len(d.Prefix) {
			return d.insert(key[len(comm):], Value)
		}
		p := &radNode{
			Prefix:   d.Prefix[len(comm):],
			Value:    d.Value,
			Children: d.Children,
		}
		n := &radNode{
			Prefix: key[len(comm):],
			Value:  Value,
		}
		d.Prefix = comm
		d.Children = make([]*radNode, 0)
		d.Children = append(d.Children, p)
		d.Children = append(d.Children, n)
		d.Value = nil
		return nil
	}
	n := &radNode{
		Prefix: key,
		Value:  Value,
	}
	r.Children = append(r.Children, n)
	return nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) Lookup(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	if x, ok := rad.Root.lookup(key); ok {
		return x.Value
	}
	return nil
}

//todo: support marker & remove duplicate, see TestLookupByPrefixAndDelimiter_complex
func (rad *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, count int, limitLevel int) *list.List {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	n, _ := rad.Root.lookup(prefix)
	if n == nil {
		return list.New()
	}
	println(n.Prefix, "---", n.Value)

	return n.getFirstByDelimiter(delimiter, count, limitLevel)
}

// Prefix returns a list of elements that share a given prefix.
func (rad *Radix) Prefix(prefix string) *list.List {
	rad.lock.Lock()
	defer rad.lock.Unlock()
	l := list.New()
	n, _ := rad.Root.lookup(prefix)
	if n == nil {
		return l
	}
	n.addToList(l)
	return l
}

// add the content of a node and its Childrenendants to a list
func (r *radNode) addToList(l *list.List) {
	if r.Value != nil {
		l.PushBack(r.Value)
	}
	for _, d := range r.Children {
		d.addToList(l)
	}
}

func (r *radNode) getFirstByDelimiter(delimiter string, count int, limitLevel int) *list.List {
	l := list.New()
	for _, d := range r.Children {
		//leaf or prefix include delimiter
		println("check ", d.Prefix, "--")
		if len(d.Children) == 0 { //leaf node
			if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
				println("delimiter ", delimiter, " found")
				l.PushBack(d.Prefix[:pos])
				//no need to search sub tree
				continue
			}

			l.PushBack(d.Prefix)
			continue
		}

		println("check delimiter ", d.Prefix, delimiter)
		if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
			println("delimiter ", delimiter, " found")
			l.PushBack(d.Prefix[:pos])
			//no need to search sub tree
			continue
		} else {
			l.PushBack(d.Prefix)
			ll := d.getFirstByDelimiter(delimiter, count, limitLevel)
			for e := ll.Front(); e != nil; e = e.Next() {
				l.PushBack(e.Value.(string))
			}
		}
	}

	moreCompleteList := list.New()
	for e := l.Front(); e != nil; e = e.Next() {
		moreCompleteList.PushBack(r.Prefix + e.Value.(string))
	}

	return moreCompleteList
}

// implementats lookup
func (r *radNode) lookup(key string) (*radNode, bool) {
	for _, d := range r.Children {
		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				return d, true
			}
			return d, false
		}
		return d.lookup(key[len(comm):])
	}
	return nil, false
}

// return the common string
func common(s, o string) string {
	max, min := s, o
	if len(max) < len(min) {
		max, min = min, max
	}
	var str []rune
	for i, r := range min {
		if r != rune(max[i]) {
			break
		}
		if str == nil {
			str = []rune{r}
		} else {
			str = append(str, r)
		}
	}
	return string(str)
}
