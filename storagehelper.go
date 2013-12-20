package radix

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
)

var startSeq int64 = ROOT_SEQ
var store = &Levelstorage{} //todo: let call to register storage
var loadmu sync.Mutex

func allocSeq() int64 {
	startSeq = startSeq + 1
	err := store.SaveLastSeq(startSeq)
	if err != nil {
		log.Fatal(err)
	}

	// log.Println("alloc seq", seq)
	return startSeq
}

func persistentNode(n radNode, value []byte) error {
	children := n.cloneChildren()

	seq := strconv.FormatInt(n.Seq, 10)
	n.InDisk = true
	n.Children = children
	buf, err := json.Marshal(n)
	if err != nil {
		log.Fatal(err)
		return err
	}

	// log.Println("persistentNode", n.Value, string(buf))
	if err = store.WriteNode(seq, buf); err != nil {
		log.Fatal(err)
	}

	if len(n.Value) > 0 && value != nil { //key exist
		// log.Println("putkey", n.Value, string(value))
		if err = store.PutKey(n.Value, value); err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

func delNodeFromStorage(seq int64) error {
	seqStr := strconv.FormatInt(seq, 10)
	if err := store.DelNode(seqStr); err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

func delFromStoragebyKey(key string) error {
	err := store.DeleteKey(key)
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func GetValueFromStore(key string) ([]byte, error) {
	return store.GetKey(key)
}

func getChildrenByNode(n *radNode) error {
	loadmu.Lock() //todo: using seq and hashring to make lock less heivy
	defer loadmu.Unlock()

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
	buf, err := store.ReadNode(seqstr)
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

func DumpNode(node *radNode, level int) error {
	if node == nil {
		return nil
	}

	if node.InDisk {
		getChildrenByNode(node)
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
		DumpNode(n, level+1)
	}

	return nil
}
