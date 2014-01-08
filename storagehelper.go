package radix

import (
	"fmt"
	"github.com/ngaut/logging"
	//enc "labix.org/v2/mgo/bson"
	enc "encoding/json"
	"reflect"
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
	seq := atomic.AddInt64(&self.startSeq, 1)
	err := self.store.SaveLastSeq(seq)
	if err != nil {
		logging.Fatal(err)
	}

	logging.Info("alloc seq", seq)
	return seq
}

func (self *helper) persistentNode(n radNode, value []byte) error {
	children := n.cloneChildren()

	seq := strconv.FormatInt(n.Seq, 10)
	n.OnDisk = true
	n.Children = children
	buf, err := enc.Marshal(n)
	if err != nil {
		logging.Fatal(err)
		return err
	}

	// logging.Println("persistentNode", n.Value, string(buf))
	if err = self.store.WriteNode(seq, buf); err != nil {
		logging.Fatal(err)
	}

	if len(n.Value) > 0 && value != nil { //key exist
		// logging.Println("putkey", n.Value, string(value))
		if err = self.store.PutKey(n.Value, value); err != nil {
			logging.Fatal(err)
			return err
		}
	}

	return nil
}

func (self *helper) delNodeFromStorage(seq int64) error {
	seqStr := strconv.FormatInt(seq, 10)
	if err := self.store.DelNode(seqStr); err != nil {
		logging.Fatal(err)
		return err
	}

	self.AddInMemoryNodeCount(-1)

	return nil
}

func (self *helper) delFromStoragebyKey(key string) error {
	err := self.store.DeleteKey(key)
	if err != nil {
		logging.Fatal(err)
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
	self.loadmu.Lock() //todo: using seq and lockring to make lock less heavy
	defer self.loadmu.Unlock()

	if !n.OnDisk { //check if multithread loading the same node
		return nil
	}
	//debug msg
	// if n.father != nil {
	// 	logging.Println("getChildrenByNode", n.Seq, n.father.Seq)
	// } else {
	// 	logging.Println("getChildrenByNode", n.Seq)
	// }

	father := n.father
	seq := n.Seq
	seqstr := strconv.FormatInt(n.Seq, 10)
	buf, err := self.store.ReadNode(seqstr)
	if err != nil {
		logging.Fatal(err, n.Seq)
		return err
	}

	if buf == nil {
		if seq != ROOT_SEQ {
			panic("")
			logging.Fatal("can't be real", "read node", seqstr)
		}

		return fmt.Errorf("get key %s failed", seqstr)
	}

	var tmp radNode
	err = enc.Unmarshal(buf, &tmp)
	if err != nil {
		logging.Fatal(err)
	}

	if tmp.Children != nil {
		n.Children = make([]*radNode, len(tmp.Children), len(tmp.Children))
		copy(n.Children, tmp.Children)
	}

	n.father = nil
	if !reflect.DeepEqual(*n, tmp) {
		logging.Debugf("%+v, %+v", *n, tmp)

		n.father = father
		for _, e := range n.father.Children {
			logging.Debugf("%+v", e)
		}
		panic("can't be real")
	}
	// *n = tmp

	self.AddInMemoryNodeCount(len(n.Children))

	n.father = father
	for _, c := range n.Children {
		c.father = n
		if !c.OnDisk {
			panic("")
		}
	}

	//check
	if n.Seq != seq {
		logging.Fatal("can't be real")
	}

	// logging.Infof("load from disk %+v", *n)

	n.OnDisk = false

	return err
}

func (r *radNode) cloneChildren() []*radNode {
	nodes := make([]*radNode, 0)
	for _, d := range r.Children {
		e := &radNode{}
		*e = *d //copy it
		e.Children = nil
		e.OnDisk = true
		nodes = append(nodes, e)
	}

	return nodes
}

func (self *helper) DumpNode(node *radNode, level int) error {
	if node == nil {
		return nil
	}

	self.getChildrenByNode(node)

	emptyPrefix := ""
	for i := 0; i < level; i++ {
		emptyPrefix += "    "
	}

	for _, n := range node.Children {
		//check
		if n.father.Seq != node.Seq {
			// logging.Println(node.Seq, n.father.Seq, n.Seq)
			panic("relation not match")
		}

		fmt.Printf("%s %s, value: %s, seq:%v, father:%v\n", emptyPrefix, n.Prefix, n.Value, n.Seq, n.father.Seq)
		self.DumpNode(n, level+1)
	}

	return nil
}

func (self *helper) DumpMemNode(node *radNode, level int) error {
	if node == nil {
		return nil
	}

	if node.OnDisk {
		return nil
	}

	emptyPrefix := ""
	for i := 0; i < level; i++ {
		emptyPrefix += "    "
	}

	for _, n := range node.Children {
		//check
		if n.father.Seq != node.Seq {
			// logging.Println(node.Seq, n.father.Seq, n.Seq)
			panic("relation not match")
		}

		fmt.Printf("%s %s, value: %s, seq:%v, father:%v\n", emptyPrefix, n.Prefix, n.Value, n.Seq, n.father.Seq)
		self.DumpNode(n, level+1)
	}

	return nil
}
