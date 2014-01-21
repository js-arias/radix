package radix

import (
	"fmt"
	"github.com/ngaut/logging"
	//enc "labix.org/v2/mgo/bson"
	// "bytes"
	// enc "encoding/json"
	"math/rand"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	maxworker = 20
)

type helper struct {
	store             Storage
	inmemoryNodeCount int64
	startSeq          int64
	reqch             chan *request
}

type readResult struct {
	n   *radNode
	err error
}

type request struct {
	seq      int64
	resultCh chan *readResult
}

func NewHelper(s Storage, startSeq int64) *helper {
	h := &helper{store: s, startSeq: startSeq, reqch: make(chan *request, 1024)}
	for i := 0; i < maxworker; i++ {
		go h.work()
	}

	return h
}

func (self *helper) work() {
	for req := range self.reqch {
		n, err := self.readRadDiskNode(req.seq)
		if err != nil {
			logging.Fatalf("should never happend %+v", req)
			req.resultCh <- &readResult{nil, err}
			continue
		}

		x := self.makeRadNode(n)
		if x.Seq != req.seq { //check
			logging.Errorf("seq not match, expect %d got %d, %+v", req.seq, x.Seq, x)
			panic("never happend")
		}
		req.resultCh <- &readResult{x, err}
	}
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

func (self *helper) makeRadDiskNode(n *radNode) *radDiskNode {
	//todo: clean up []byte<->string conversion
	return &radDiskNode{Prefix: string(n.Prefix), Children: n.cloneChildrenSeq(), Value: string(n.Value), Version: n.Version,
		Seq: n.Seq}
}

func (self *helper) makeRadNode(x *radDiskNode) *radNode {
	//todo: clean up []byte<->string conversion
	return &radNode{Prefix: []byte(x.Prefix), Children: nil, Value: []byte(x.Value), Version: x.Version,
		Seq: x.Seq, Stat: statOnDisk}
}

func (self *helper) persistentNode(n *radNode, value []byte) error {
	x := self.makeRadDiskNode(n)

	seq := strconv.FormatInt(x.Seq, 10)
	buf, err := Marshal(x) //enc.Marshal(x)
	if err != nil {
		logging.Fatal(err)
		return err
	}

	// logging.Info("persistentNode", n.Value, string(buf))
	if err = self.store.WriteNode(seq, buf); err != nil {
		logging.Fatal(err)
	}

	if len(x.Value) > 0 && value != nil { //key exist
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
	if err := self.store.DelNode([]byte(seqStr)); err != nil {
		logging.Fatal(err)
		return err
	}

	return nil
}

func (self *helper) delFromStoragebyKey(key []byte) error {
	err := self.store.DeleteKey(key)
	if err != nil {
		logging.Fatal(err)
	}

	return err
}

func (self *helper) AddInMemoryNodeCount(n int) {
	logging.Info("AddInMemoryNodeCount", n)
	atomic.AddInt64(&self.inmemoryNodeCount, int64(n))
}

func (self *helper) GetInMemoryNodeCount() int64 {
	return atomic.LoadInt64(&self.inmemoryNodeCount)
}

func (self *helper) ResetInMemoryNodeCount() {
	atomic.StoreInt64(&self.inmemoryNodeCount, 0)
}

func (self *helper) GetValueFromStore(key []byte) ([]byte, error) {
	return self.store.GetKey(key)
}

func (self *helper) readRadDiskNode(seq int64) (*radDiskNode, error) {
	buf, err := self.store.ReadNode(strconv.FormatInt(seq, 10))
	if err != nil {
		logging.Fatal(err, seq)
		return nil, err
	}

	// logging.Debug(seq, string(buf))

	if buf == nil { //when database is empty
		return nil, fmt.Errorf("get key %d failed", seq)
	}

	var x radDiskNode
	err = Unmarshal(buf, &x) //enc.Unmarshal(buf, &x)
	if err != nil {
		logging.Fatal(err)
		return nil, err
	}

	return &x, nil
}

func (self *helper) getNodeFromDisk(n *radNode) error {
	if n.Stat != statLoading { //check if multithread loading the same node
		panic("never happend")
	}

	tmp, err := self.readRadDiskNode(n.Seq)
	if err != nil {
		if n.Seq != ROOT_SEQ {
			panic(err.Error())
			logging.Fatal("can't be real", "read node", n.Seq)
		}
		if tmp == nil {
			return fmt.Errorf("get key %d failed", n.Seq)
		}
	}

	logging.Infof("%+v", tmp)
	if len(tmp.Children) > 0 {
		n.Children = make([]*radNode, len(tmp.Children), len(tmp.Children))
		self.AddInMemoryNodeCount(len(n.Children))
	} else {
		return nil
	}

	if len(tmp.Children) == 1 { //concurrent can't help, less garbage
		for i, seq := range tmp.Children {
			x, err := self.readRadDiskNode(seq)
			if err != nil { //check
				panic(err.Error())
			}
			if x.Seq != seq { //check
				logging.Errorf("seq not match, expect %d got %d, %+v", seq, x.Seq, x)
				panic("never happend")
			}

			node := self.makeRadNode(x)
			node.father = n
			n.Children[i] = node
		}
		return nil
	}

	resultCh := make(chan *readResult, len(tmp.Children))

	//send request
	for _, seq := range tmp.Children {
		self.reqch <- &request{seq: seq, resultCh: resultCh}
	}

	//read result
	for i, _ := range tmp.Children {
		res := <-resultCh
		if res.err != nil {
			panic("should never happend")
		}

		res.n.father = n
		n.Children[i] = res.n
	}

	logging.Infof("load from disk %+v", n)
	return err
}

func (self *helper) getChildrenByNode(n *radNode) error {
	for {
		stat := atomic.LoadInt64(&n.Stat)
		switch stat {
		case statOnDisk:
			if atomic.CompareAndSwapInt64(&n.Stat, statOnDisk, statLoading) { //try to get ownership
				if err := self.getNodeFromDisk(n); err != nil {
					if !atomic.CompareAndSwapInt64(&n.Stat, statLoading, statInMemory) {
						panic("never happend")
					}
					return err
				}

				if !atomic.CompareAndSwapInt64(&n.Stat, statLoading, statInMemory) {
					panic("never happend")
				}
				return nil
			} else { //someone is loading it
				n := rand.Int31n(100)
				time.Sleep(time.Duration(n) * time.Microsecond)
			}
		case statInMemory:
			return nil
		case statLoading:
			n := rand.Int31n(100)
			time.Sleep(time.Duration(n) * time.Microsecond)
		default:
			logging.Fatal("error stat", stat)
		}
	}
}

func (r *radNode) cloneChildrenSeq() []int64 {
	nodes := make([]int64, len(r.Children), len(r.Children))
	for i, d := range r.Children {
		nodes[i] = d.Seq
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

	if onDisk(node) {
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
		self.DumpMemNode(n, level+1)
	}

	return nil
}
