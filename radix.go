// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package radix implement a radix tree. It is expected that the
// keys are in UTF-8 (i.e. go runes), and that insertion and lookup
// is far more common than deletion.
package radix

import (
	"container/list"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Radix is a radix tree.
type Radix struct {
	Root *radNode   // Root of the radix tree
	lock sync.Mutex // protect the radix
	path string
}

// a node of a radix tree
type radNode struct {
	Prefix   string      `json:"prefix,omitempty"` // current prefix of the node
	Children []*radNode  `json:"children,omitempty"`
	Value    interface{} `json:"value,omitempty"` // stored Value
	Seq      int64
	InDisk   bool
}

// New returns a new, empty radix tree.
func New(path string) *Radix {
	log.Println("open db")
	rad := &Radix{
		Root: &radNode{Seq: -1},
		path: path + "/db",
	}

	if err := store.Open(rad.path); err != nil {
		log.Fatal(err)
	}

	if err := rad.Root.getChildrenByNode(rad.Root); err != nil {
		log.Println(err)
		rad.Root.persistentNode(*rad.Root)
		log.Printf("%+v", rad.Root)
	} else {
		log.Printf("%+v", rad.Root)
		startSeq, err = store.GetLastSeq()
		if err != nil {
			log.Fatal(err)
		}
	}

	return rad
}

var startSeq int64 = -1
var store = &Levelstorage{}

func AllocSeq() int64 {
	seq := atomic.AddInt64(&startSeq, 1)
	err := store.SaveLastSeq(seq)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println("alloc seq", seq)

	return seq
}

func (rad *Radix) Close() error {
	log.Println("close db")
	return store.Close()
}

func (rad *Radix) Destory() error {
	log.Println("Destory!")
	store.Close()
	os.RemoveAll(rad.path)
	return nil
}

// Delete removes the Value associated with a particular key and returns it.
func (rad *Radix) Delete(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	return rad.Root.delete(key)
}

// implements delete
// todo: need to remove it if this is leaf node
func (r *radNode) delete(key string) interface{} {
	if x, father, i, ok := r.lookup(key); ok {
		val := x.Value
		// only assign a nil, therefore skip any modification
		// of the radix topology

		//seq := x.Seq

		log.Println("delete", key, "father", father)

		if len(x.Children) > 0 {
			x.Value = nil
			x.persistentNode(*x)
			return val
		}

		//x is leaf node
		if len(father.Children) > 1 {
			father.Children = append(father.Children[:i], father.Children[:i]...)
			father.persistentNode(*father)
			log.Println("delete", key, "father", father)
		} else if len(father.Children) == 1 {
			father.Children = nil
			father.persistentNode(*father)
			//todo:remove from leveldb
		} else {
			panic("never happend")
		}

		return val
	}

	return nil
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
//todo: using transaction(batch write)
func (rad *Radix) Insert(key string, Value interface{}) error {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	return rad.Root.insert(key, Value)
}

func (r *radNode) persistentNode(n radNode) error {
	children := n.cloneChildren()

	seq := strconv.FormatInt(n.Seq, 10)
	n.InDisk = true
	n.Children = children
	buf, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println("persistentNode", string(buf))

	if err = store.WriteNode(seq, buf); err != nil {
		log.Fatal(err)
	}

	return err
}

func (r *radNode) getChildrenByNode(n *radNode) error {
	seq := n.Seq
	seqstr := strconv.FormatInt(n.Seq, 10)
	buf, err := store.ReadNode(seqstr)
	if err != nil {
		log.Println(err, n.Seq)
		return err
	}

	err = json.Unmarshal(buf, n)
	if err != nil {
		log.Fatal(err)
	}

	n.InDisk = false

	//check
	if n.Seq != seq {
		log.Fatal("can't be ")
	}

	// log.Printf("%+v\n", n)

	return err
}

func (r *radNode) cloneChildren() []*radNode {
	nodes := make([]*radNode, 0)
	for _, d := range r.Children {
		e := &radNode{}
		*e = *d //copy it
		e.Children = nil
		e.InDisk = true
		nodes = append(nodes, e)
	}

	return nodes
}

// implements insert
func (r *radNode) insert(key string, Value interface{}) error {
	// log.Println("insert", key)
	if r.InDisk {
		log.Printf("get %+v", r)
		r.getChildrenByNode(r)
	}

	for _, d := range r.Children {
		// log.Println("d.Prefix", d.Prefix)
		if d.InDisk {
			checkprefix := d.Prefix
			d.getChildrenByNode(d)
			if d.Prefix != checkprefix {
				log.Fatal("can't be")
			}
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}

		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				if d.Value == nil {
					log.Printf("set seq %d %s value", d.Seq, Value)
					d.Value = Value
					d.persistentNode(*d)
					return nil
				}
				log.Printf("%s key already in use", Value)
				return errors.New("key already in use")
			}

			n := &radNode{
				Prefix:   d.Prefix[len(comm):],
				Value:    d.Value,
				Children: d.Children,
				Seq:      AllocSeq(),
			}

			n.persistentNode(*n)

			d.Children = make([]*radNode, 1, 1)
			d.Children[0] = n
			d.Prefix = comm
			d.Value = Value
			d.persistentNode(*d)
			return nil
		}

		if len(comm) == len(d.Prefix) {
			return d.insert(key[len(comm):], Value)
		}

		//ex: ab, insert ac, extra common a

		p := &radNode{
			Prefix:   d.Prefix[len(comm):],
			Value:    d.Value,
			Children: d.Children,
			Seq:      AllocSeq(),
		}

		p.persistentNode(*p)
		n := &radNode{
			Prefix: key[len(comm):],
			Value:  Value,
			Seq:    AllocSeq(),
		}

		n.persistentNode(*n)

		d.Prefix = comm
		d.Children = make([]*radNode, 2, 2)
		d.Children[0] = p
		d.Children[1] = n
		d.Value = nil
		d.persistentNode(*d)
		return nil
	}

	n := &radNode{
		Prefix: key,
		Value:  Value,
		Seq:    AllocSeq(),
	}
	r.persistentNode(*n)
	r.Children = append(r.Children, n)
	r.persistentNode(*r)

	return nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) Lookup(key string) interface{} {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	if x, _, _, ok := rad.Root.lookup(key); ok {
		return x.Value
	}
	return nil
}

//todo: support marker & remove duplicate, see TestLookupByPrefixAndDelimiter_complex
func (rad *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, limitCount int32, limitLevel int, marker string) *list.List {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	println("limitCount", limitCount)

	node, _, _, _ := rad.Root.lookup(prefix)
	if node == nil {
		return list.New()
	}
	// println(node.Prefix, "---", node.Value)

	var currentCount int32

	return node.getFirstByDelimiter(marker, delimiter, limitCount, limitLevel, &currentCount)
}

// Prefix returns a list of elements that share a given prefix.
func (rad *Radix) Prefix(prefix string) *list.List {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	l := list.New()
	n, _, _, _ := rad.Root.lookup(prefix)
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

//return: false if full
func save(l *list.List, str string, marker string, value interface{}, limitCount int32, currentCount *int32, inc bool) bool {
	if inc {
		if atomic.LoadInt32(currentCount) >= limitCount {
			println("full")
			return false
		}
	}

	if str > marker && value != nil {
		// println("add ", str)
		l.PushBack(str)
		if inc {
			atomic.AddInt32(currentCount, 1)
		}
	}

	return true
}

func (r *radNode) getFirstByDelimiter(marker string, delimiter string, limitCount int32, limitLevel int, currentCount *int32) *list.List {
	l := list.New()
	// println("===> prefix: ", r.Prefix, "marker ", marker, "level: ", limitLevel)
	// defer func() {
	// 	println("exit level ", limitLevel)
	// }()

	if r.InDisk {
		r.getChildrenByNode(r)
	}

	//search root first
	if pos := strings.Index(r.Prefix, delimiter); pos >= 0 {
		// println("delimiter ", delimiter, " found")
		save(l, r.Prefix[:pos+1], marker, true, limitCount, currentCount, true)
		return l
	}

	n := len(common(marker, r.Prefix))
	marker = marker[n:]

L:
	for _, d := range r.Children {
		//leaf or prefix include delimiter
		// println("check ", d.Prefix, "marker ", marker)

		if d.InDisk {
			d.getChildrenByNode(d)
		}

		if len(d.Children) == 0 { //leaf node
			// println("leaf: ", d.Prefix)
			if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
				// println("delimiter ", delimiter, " found")
				if !save(l, d.Prefix[:pos+1], marker, true, limitCount, currentCount, true) {
					break L
				}

				//no need to search sub tree
				continue
			}

			if !save(l, d.Prefix, marker, true, limitCount, currentCount, true) {
				break L
			}

			continue
		}

		// println("check delimiter ", d.Prefix, delimiter)
		if pos := strings.Index(d.Prefix, delimiter); pos >= 0 {
			println("delimiter ", delimiter, " found")
			if !save(l, d.Prefix[:pos+1], marker, true, limitCount, currentCount, true) {
				break L
			}

			//no need to search sub tree
			continue
		} else {
			if !save(l, d.Prefix, marker, d.Value, limitCount, currentCount, true) {
				break L
			}

			n := len(common(marker, r.Prefix))
			ll := d.getFirstByDelimiter(marker[n:], delimiter, limitCount, limitLevel+1, currentCount)
			for e := ll.Front(); e != nil; e = e.Next() { //no need to check full, already checked by child function
				save(l, e.Value.(string), marker, true, limitCount, currentCount, false)
			}
		}
	}

	moreCompleteList := list.New()
	for e := l.Front(); e != nil; e = e.Next() {
		// println("level:", limitLevel, "moreCompleteList", r.Prefix+e.Value.(string))
		moreCompleteList.PushBack(r.Prefix + e.Value.(string))
	}

	return moreCompleteList
}

// implementats lookup: node, father, exist, index
func (r *radNode) lookup(key string) (*radNode, *radNode, int, bool) {
	if r.InDisk {
		r.getChildrenByNode(r)
	}

	for i, d := range r.Children {
		if d.InDisk {
			d.getChildrenByNode(d)
			log.Printf("get %+v", d)
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				return d, r, i, true
			}
			return d, nil, i, false
		}
		return d.lookup(key[len(comm):])
	}
	return nil, nil, 0, false
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
