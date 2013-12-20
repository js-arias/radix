// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

// Package radix implement a radix tree. It is expected that the
// keys are in UTF-8 (i.e. go runes), and that insertion and lookup
// is far more common than deletion.
package radix

import (
	"container/list"
	"fmt"
	"log"
	"strings"
)

//todo:
// api
// cut edge, limit count of nodes in memory
// gc performance test
// read write lock, load from disk thread safe

// a node of a radix tree
type radNode struct {
	Prefix   string     `json:"prefix,omitempty"` // current prefix of the node
	Children []*radNode `json:"children,omitempty"`
	Value    string     `json:"value,omitempty"` // stored key
	Version  int64
	father   *radNode
	Seq      int64
	InDisk   bool
}

func (rad *Radix) beginWriteBatch() {
	store.BeginWriteBatch()
}

func (rad *Radix) commitWriteBatch() error {
	return store.CommitWriteBatch()
}

func (rad *Radix) rollback() error {
	return store.Rollback()
}

func deleteNode(n *radNode) {
	if n == nil {
		return
	}

	if n.Seq == ROOT_SEQ { //root
		persistentNode(*n, nil)
		return
	}

	// log.Println(n.Seq, n.father.Seq)
	//remove from storage
	if len(n.Value) > 0 {
		err := delFromStoragebyKey(n.Value)
		if err != nil {
			log.Fatal(err)
		}
		n.Value = ""
	}

	if len(n.Children) > 0 {
		// log.Println(n.Seq, n.father.Seq)
		err := persistentNode(*n, nil)
		if err != nil {
			log.Fatal(err)
		}

		return
	}

	//now, n has no children, check if we need to clean father
	//todo: binary search
	i := 0
	for ; i < len(n.father.Children); i++ { //get index
		if n.father.Children[i].Seq == n.Seq {
			break
		}
	}

	delNodeFromStorage(n.Seq)

	//n is leaf node
	if len(n.father.Children) > 1 {
		if i == len(n.father.Children)-1 { //last one
			n.father.Children[i] = nil
			n.father.Children = n.father.Children[:i]
		} else {
			n.father.Children = append(n.father.Children[:i], n.father.Children[i+1:]...)
		}

		persistentNode(*n.father, nil)
		//todo: if there is only node after remove, we can do combine
	} else if len(n.father.Children) == 1 {
		// log.Println("recursive delete")
		n.father.Children = nil

		if len(n.father.Value) == 0 {
			deleteNode(n.father) //recursive find & delete
		} else {
			persistentNode(*n.father, nil)
		}
	} else {
		log.Println(n.Seq, n.father.Seq)
		panic("never happend")
	}
}

// implements delete
func (r *radNode) delete(key string) []byte {
	if x, _, ok := r.lookup(key); ok {
		v, err := GetValueFromStore(x.Value)
		if err != nil {
			log.Fatal("never happend")
		}

		// log.Printf("delete %s father %+v", key, father)
		// log.Printf("delete %v father %v", x.Seq, father.Seq)

		deleteNode(x)

		return v
	}

	return nil
}

// implements insert or replace, return nil, nil if this a new value
func (r *radNode) put(key string, Value []byte, orgKey string, version int64, force bool) ([]byte, error) {
	// log.Println("insert", orgKey, "--", string(Value))
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
				if len(d.Value) == 0 {
					d.Value = encodeValueToInternalKey(orgKey)
					persistentNode(*d, Value)
					return nil, nil
				}

				if force || version == d.Version {
					d.Value = encodeValueToInternalKey(orgKey)
					orgValue, err := GetValueFromStore(d.Value)
					if err != nil {
						log.Fatal(err)
					}
					d.Version++
					persistentNode(*d, Value)
					return orgValue, nil
				}

				// log.Printf("version not match, version is %d, but you provide %d", d.Version, version)
				return nil, fmt.Errorf("version not match, version is %d, but you provide %d", d.Version, version)
			}

			//ex: ab, insert a
			n := &radNode{
				Prefix:   d.Prefix[len(comm):],
				Value:    d.Value,
				father:   d,
				Children: d.Children,
				Seq:      allocSeq(),
			}
			//adjust father
			for _, x := range n.Children {
				x.father = n
			}

			persistentNode(*n, nil)

			d.Children = make([]*radNode, 1, 1)
			d.Children[0] = n
			d.Prefix = comm
			d.Value = encodeValueToInternalKey(orgKey)
			persistentNode(*d, Value)
			return nil, nil
		}

		//ex: a, insert ab
		if len(comm) == len(d.Prefix) {
			return d.put(key[len(comm):], Value, orgKey, version, force)
		}

		//ex: ab, insert ac, extra common a
		p := &radNode{
			Prefix:   d.Prefix[len(comm):],
			Value:    d.Value,
			father:   d,
			Children: d.Children,
			Seq:      allocSeq(),
		}
		//adjust father
		for _, x := range p.Children {
			x.father = p
		}

		persistentNode(*p, nil)
		n := &radNode{
			Prefix: key[len(comm):],
			Value:  encodeValueToInternalKey(orgKey),
			father: d,
			Seq:    allocSeq(),
		}

		persistentNode(*n, Value)

		d.Prefix = comm
		d.Value = ""
		d.Children = make([]*radNode, 2, 2)
		d.Children[0] = p
		d.Children[1] = n

		persistentNode(*d, nil)
		return nil, nil
	}

	n := &radNode{
		Prefix: key,
		Value:  encodeValueToInternalKey(orgKey),
		father: r,
		Seq:    allocSeq(),
	}
	persistentNode(*n, Value)
	r.Children = append(r.Children, n)
	persistentNode(*r, nil)

	return nil, nil
}

// add the content of a node and its Childrenendants to a list
func (r *radNode) addToList(l *list.List) {
	if len(r.Value) > 0 {
		l.PushBack(decodeValueToKey(r.Value))
	}
	for _, d := range r.Children {
		d.addToList(l)
	}
}

//return: false if full
func save(l *list.List, str string, marker string, value interface{}, limitCount int32, currentCount *int32, inc bool) bool {
	if inc {
		if *currentCount >= limitCount {
			return false
		}
	}

	if str > marker && value != nil {
		// println("add ", str)
		l.PushBack(str)
		if inc {
			*currentCount = *currentCount + 1
		}
	}

	return true
}

func (r *radNode) getFirstByDelimiter(marker string, delimiter string, limitCount int32, limitLevel int, currentCount *int32) *list.List {
	l := list.New()

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

// implementats lookup: node, index, exist
func (r *radNode) lookup(key string) (*radNode, int, bool) {
	if r.InDisk {
		getChildrenByNode(r)
		// log.Printf("get from disk %+v, searching %s", r, key)
	}

	// log.Println("lookup", key)

	for i, d := range r.Children {
		if d.InDisk {
			getChildrenByNode(d)
			// log.Printf("get from disk %+v, searching %s", d, key)
		}

		comm := common(key, d.Prefix)
		if len(comm) == 0 {
			continue
		}
		// The key is found
		if len(comm) == len(key) {
			if len(comm) == len(d.Prefix) {
				// log.Println("found", d.Value)
				return d, i, true
			}
			return d, i, false
		}
		return d.lookup(key[len(comm):])
	}
	return nil, 0, false
}
