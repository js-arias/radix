package radix

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
)

type helper struct {
	store             Storage
	loadmu            sync.Mutex
	inmemoryNodeCount int64
	startSeq          int64
}

func (self *helper) allocSeq() int64 {
	self.startSeq += 1
	err := self.store.SaveLastSeq(self.startSeq)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println("alloc seq", seq)
	return self.startSeq
}

func (self *helper) persistentNode(n radNode, value []byte) error {
	children := n.cloneChildren()

	seq := strconv.FormatInt(n.Seq, 10)
	n.InDisk = true
	n.Children = children
	buf, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.Println("persistentNode", n.Value, string(buf))
	if err = self.store.WriteNode(seq, buf); err != nil {
		log.Fatal(err)
	}

	if len(n.Value) > 0 && value != nil { //key exist
		// log.Println("putkey", n.Value, string(value))
		if err = self.store.PutKey(n.Value, value); err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

func (self *helper) delNodeFromStorage(seq int64) error {
	seqStr := strconv.FormatInt(seq, 10)
	if err := self.store.DelNode(seqStr); err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func (self *helper) delFromStoragebyKey(key string) error {
	err := self.store.DeleteKey(key)
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func (self *helper) AddInMemoryNodeCount(n int) {
	atomic.AddInt64(&self.inmemoryNodeCount, int64(n))
}

func (self *helper) GetInMemoryNodeCount() int64 {
	return atomic.LoadInt64(&self.inmemoryNodeCount)
}

func (self *helper) ResetInMemoryNodeCount() {
	atomic.StoreInt64(&self.inmemoryNodeCount, 0)
}

func (self *helper) GetValueFromStore(key string) ([]byte, error) {
	return self.store.GetKey(key)
}

func (self *helper) getChildrenByNode(n *radNode) error {
	self.loadmu.Lock() //todo: using seq and hashring to make lock less heivy
	defer self.loadmu.Unlock()

	if !n.InDisk { //check if multithread loading the same node
		return nil
	}
	//debug msg
	// if n.father != nil {
	// 	log.Println("getChildrenByNode", n.Seq, n.father.Seq)
	// } else {
	// 	log.Println("getChildrenByNode", n.Seq)
	// }

	father := n.father
	seq := n.Seq
	seqstr := strconv.FormatInt(n.Seq, 10)
	buf, err := self.store.ReadNode(seqstr)
	if err != nil {
		log.Println(err, n.Seq)
		return err
	}

	if buf == nil {
		return errors.New("get key failed")
	}

	err = json.Unmarshal(buf, n)
	if err != nil {
		log.Fatal(err)
	}

	self.AddInMemoryNodeCount(len(n.Children))

	n.father = father
	for _, x := range n.Children {
		x.father = n
	}

	//check
	if n.Seq != seq {
		log.Fatal("can't be real")
	}

	n.InDisk = false

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

func (self *helper) DumpNode(node *radNode, level int) error {
	if node == nil {
		return nil
	}

	loadFromDisk := false

	if node.InDisk {
		self.getChildrenByNode(node)
		// log.Printf("load: %+v", node)
		loadFromDisk = true
	}

	emptyPrefix := ""
	for i := 0; i < level; i++ {
		emptyPrefix += "    "
	}

	for _, n := range node.Children {
		//check
		if n.father.Seq != node.Seq {
			log.Println(node.Seq, n.father.Seq, n.Seq)
			panic("relation not match")
		}

		fmt.Printf("%s %s, value: %s, seq:%v, father:%v, loadFromDisk:%v\n", emptyPrefix, n.Prefix, n.Value, n.Seq, n.father.Seq, loadFromDisk)
		self.DumpNode(n, level+1)
	}

	return nil
}

func (self *helper) DumpMemNode(node *radNode, level int) error {
	if node == nil {
		return nil
	}

	if node.InDisk {
		return nil
	}

	emptyPrefix := ""
	for i := 0; i < level; i++ {
		emptyPrefix += "    "
	}

	for _, n := range node.Children {
		//check
		if n.father.Seq != node.Seq {
			log.Println(node.Seq, n.father.Seq, n.Seq)
			panic("relation not match")
		}

		fmt.Printf("%s %s, value: %s, seq:%v, father:%v\n", emptyPrefix, n.Prefix, n.Value, n.Seq, n.father.Seq)
		self.DumpNode(n, level+1)
	}

	return nil
}
