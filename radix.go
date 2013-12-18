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
	"log"
	"os"
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
	Value    interface{} `json:"value,omitempty"` // stored key
	father   *radNode
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

	if err := getChildrenByNode(rad.Root); err != nil {
		log.Println(err)
		persistentNode(*rad.Root, nil)
		log.Printf("root: %+v", rad.Root)
	} else {
		log.Printf("root: %+v", rad.Root)
		startSeq, err = store.GetLastSeq()
		if err != nil {
			log.Fatal(err)
		}
	}

	return rad
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

func (rad *Radix) DumpTree() error {
	if rad.Root == nil {
		return nil
	}

	log.Println("dump tree:")

	DumpNode(rad.Root, 0)

	return nil
}

// Delete removes the Value associated with a particular key and returns it.
//todo: using transaction
func (rad *Radix) Delete(key string) []byte {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	return rad.Root.delete(key)
}

func deleteNode(n *radNode) {
	if n == nil {
		return
	}

	//remove from storage
	if n.Value != nil {
		err := delFromStoragebyKey(n.Value.(string))
		if err != nil {
			log.Fatal(err)
		}
		n.Value = nil
	}

	if len(n.Children) > 0 {
		err := persistentNode(*n, nil)
		if err != nil {
			log.Fatal(err)
		}

		return
	}

	//now, n has no children, check if we need to clean father

	//get index
	//todo: binary search
	i := 0
	for ; i < len(n.father.Children); i++ {
		if n.father.Children[i].Seq == n.Seq {
			break
		}
	}

	//n is leaf node
	if len(n.father.Children) > 1 {
		delNodeFromStorage(n.Seq)
		n.father.Children = append(n.father.Children[:i], n.father.Children[i+1:]...)
		persistentNode(*n.father, nil)
		//todo: if there is only node after remove, we can do combine
	} else if len(n.father.Children) == 1 { //todo: recursive find & delete
		delNodeFromStorage(n.Seq)
		n.father.Children = nil

		if n.father.Value == nil {
			deleteNode(n.father)
		} else {
			persistentNode(*n.father, nil)
		}
	} else {
		panic("never happend")
	}
}

// implements delete
func (r *radNode) delete(key string) []byte {
	if x, father, _, ok := r.lookup(key); ok {
		v, err := GetValueFromStore(x.Value.(string))
		if err != nil {
			log.Fatal("never happend")
		}

		log.Printf("delete %s father %+v", key, father)

		deleteNode(x)

		return v
	}

	return nil
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
//todo: using transaction(batch write)
func (rad *Radix) Insert(key string, Value string) error {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	return rad.Root.insert(key, []byte(Value), key)
}

// implements insert
func (r *radNode) insert(key string, Value []byte, orgKey string) error {
	// log.Println("insert", key)
	if r.InDisk {
		log.Printf("get %+v", r)
		getChildrenByNode(r)
	}

	for _, d := range r.Children {
		// log.Println("d.Prefix", d.Prefix)
		if d.InDisk {
			checkprefix := d.Prefix
			getChildrenByNode(d)
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
					d.Value = orgKey
					persistentNode(*d, Value)
					return nil
				}
				log.Printf("%s key already in use", orgKey)
				return errors.New("key already in use")
			}

			//ex: ab, insert a

			n := &radNode{
				Prefix:   d.Prefix[len(comm):],
				Value:    d.Value,
				father:   d,
				Children: d.Children,
				Seq:      AllocSeq(),
			}

			persistentNode(*n, nil)

			d.Children = make([]*radNode, 1, 1)
			d.Children[0] = n
			d.Prefix = comm
			d.Value = orgKey
			persistentNode(*d, Value)
			return nil
		}

		if len(comm) == len(d.Prefix) {
			return d.insert(key[len(comm):], Value, orgKey)
		}

		//ex: ab, insert ac, extra common a

		p := &radNode{
			Prefix:   d.Prefix[len(comm):],
			Value:    d.Value,
			father:   d,
			Children: d.Children,
			Seq:      AllocSeq(),
		}

		persistentNode(*p, nil)
		n := &radNode{
			Prefix: key[len(comm):],
			Value:  orgKey,
			father: d,
			Seq:    AllocSeq(),
		}

		persistentNode(*n, Value)

		d.Prefix = comm
		d.Value = nil
		d.Children = make([]*radNode, 2, 2)
		d.Children[0] = p
		d.Children[1] = n

		persistentNode(*d, nil)
		return nil
	}

	n := &radNode{
		Prefix: key,
		Value:  orgKey,
		father: r,
		Seq:    AllocSeq(),
	}
	persistentNode(*n, Value)
	r.Children = append(r.Children, n)
	persistentNode(*r, nil)

	return nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) Lookup(key string) []byte {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	if x, _, _, ok := rad.Root.lookup(key); ok {
		buf, err := GetValueFromStore(x.Value.(string))
		if err != nil {
			return nil
		}
		return buf
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
		getChildrenByNode(r)
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
			getChildrenByNode(d)
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

// implementats lookup: node, father, index, exist
func (r *radNode) lookup(key string) (*radNode, *radNode, int, bool) {
	if r.InDisk {
		getChildrenByNode(r)
		log.Printf("get from disk %+v, searching %s", r, key)
	}

	// log.Println("lookup", key)

	for i, d := range r.Children {
		if d.InDisk {
			getChildrenByNode(d)
			log.Printf("get from disk %+v, searching %s", d, key)
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				log.Println("found", d.Value)
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
