package radix

import (
	"fmt"
	"github.com/ngaut/logging"
	//enc "labix.org/v2/mgo/bson"
	// "bytes"
	enc "encoding/json"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxworker = 200
)

type helper struct {
	store             Storage
	inmemoryNodeCount int64
	startSeq          int64
	reqch             chan request //create less object
	persistentCh      chan *persistentArg
}

type readResult struct {
	n   *radNode
	err error
}

type request struct {
	seq      int64
	buf      []byte
	resultCh chan *readResult
	snapshot interface{}
}

type persistentArg struct {
	n     *radNode
	value []byte
	wg    *sync.WaitGroup
}

func NewHelper(s Storage, startSeq int64) *helper {
	h := &helper{store: s, startSeq: startSeq, reqch: make(chan request, 1024),
		persistentCh: make(chan *persistentArg, 3)}
	for i := 0; i < maxworker; i++ {
		go h.work()
	}

	for i := 0; i < 3; i++ {
		go h.persistentWorker()
	}

	return h
}

func (self *helper) work() {
	for req := range self.reqch {
		n, err := self.unmarshal2radDiskNode(req.buf)
		if err != nil {
			logging.Fatalf("should never happend %+v", req)
			req.resultCh <- &readResult{nil, err}
			continue
		}

		x := self.makeRadNode(n, req.seq)
		req.resultCh <- &readResult{x, err}
	}
}

func (self *helper) persistentWorker() {
	for arg := range self.persistentCh {
		self.persistentNode(arg.n, arg.value)
		arg.wg.Done()
	}
}

func (self *helper) allocSeq() int64 {
	seq := atomic.AddInt64(&self.startSeq, 1)
	err := self.store.SaveLastSeq(seq)
	if err != nil {
		logging.Fatal(err)
	}

	// logging.Info("alloc seq", seq)
	return seq
}

func (self *helper) makeRadDiskNode(n *radNode) *radDiskNode {
	return &radDiskNode{Prefix: string(n.Prefix), Children: n.cloneChildrenSeq(), Value: string(n.Value), Version: n.Version}
}

func (self *helper) makeRadNode(x *radDiskNode, seq int64) *radNode {
	stat := int64(statOnDisk)
	if len(x.Children) == 0 {
		stat = statInMemory
	}

	return &radNode{Prefix: []byte(x.Prefix), Value: []byte(x.Value), Version: x.Version,
		Seq: seq, Stat: stat}
}

func (self *helper) persistentNode(n *radNode, value []byte) error {
	x := self.makeRadDiskNode(n)

	seq := strconv.FormatInt(n.Seq, 10)
	buf, err := enc.Marshal(x)
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
	atomic.AddInt64(&self.inmemoryNodeCount, int64(n))
}

func (self *helper) GetInMemoryNodeCount() int64 {
	return atomic.LoadInt64(&self.inmemoryNodeCount)
}

func (self *helper) ResetInMemoryNodeCount() {
	atomic.StoreInt64(&self.inmemoryNodeCount, 0)
}

func (self *helper) GetValueFromStore(key []byte, snapshot interface{}) ([]byte, error) {
	return self.store.GetKey(key, snapshot)
}

func (self *helper) doRead(seq int64, snapshot interface{}) ([]byte, error) {
	buf, err := self.store.ReadNode(strconv.FormatInt(seq, 10), snapshot)
	if err != nil {
		logging.Fatal(err, seq)
		return nil, err
	}

	// logging.Debug(seq, string(buf))

	if buf == nil { //when database is empty
		return nil, fmt.Errorf("get key %d failed, is database empty?", seq)
	}

	return buf, err
}

func (self *helper) unmarshal2radDiskNode(buf []byte) (*radDiskNode, error) {
	var x radDiskNode
	err := enc.Unmarshal(buf, &x)
	if err != nil {
		logging.Fatal(err)
		return nil, err
	}

	return &x, nil
}

func (self *helper) readRadDiskNode(seq int64, snapshot interface{}) (*radDiskNode, error) {
	buf, err := self.doRead(seq, snapshot)
	if err != nil {
		return nil, err
	}

	return self.unmarshal2radDiskNode(buf)
}

func (self *helper) getChildren(tmp *radDiskNode, seq int64) []*radNode {
	// logging.Infof("%+v", tmp)
	var children []*radNode

	if len(tmp.Children) > 0 {
		children = make([]*radNode, len(tmp.Children), len(tmp.Children))
	} else {
		return nil
	}

	resultCh := make(chan *readResult, len(tmp.Children))

	//send request
	for _, seq := range tmp.Children {
		buf, err := self.doRead(seq, nil)
		if err != nil {
			logging.Fatal(err)
		}
		self.reqch <- request{seq: seq, buf: buf, resultCh: resultCh}
	}

	//read result
	for i, _ := range tmp.Children {
		res := <-resultCh
		if res.err != nil {
			panic("should never happend")
		}

		children[i] = res.n
	}

	return children
}

func (self *helper) getNodeFromDisk(n *radNode, snapshot interface{}) error {
	if n.Stat != statLoading { //check if multithread loading the same node
		panic("never happend")
	}

	tmp, err := self.readRadDiskNode(n.Seq, snapshot)
	if err != nil {
		if n.Seq != ROOT_SEQ {
			panic(err.Error())
			logging.Fatal("can't be real", "read node", n.Seq)
		}
		if tmp == nil {
			return fmt.Errorf("get key %d failed", n.Seq)
		}
	}

	n.Children = self.getChildren(tmp, n.Seq)
	for _, e := range n.Children {
		e.father = n
	}

	self.AddInMemoryNodeCount(len(n.Children))

	// logging.Infof("load from disk %+v", n)
	return nil
}

func (self *helper) getChildrenByNode(n *radNode, snapshot interface{}) error {
	for {
		stat := atomic.LoadInt64(&n.Stat)
		switch stat {
		case statOnDisk:
			if atomic.CompareAndSwapInt64(&n.Stat, statOnDisk, statLoading) { //try to get ownership
				if err := self.getNodeFromDisk(n, snapshot); err != nil {
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
				n := rand.Int31n(20)
				time.Sleep(time.Duration(n) * time.Microsecond)
			}
		case statInMemory:
			return nil
		case statLoading:
			n := rand.Int31n(20)
			time.Sleep(time.Duration(n) * time.Microsecond)
		default:
			logging.Fatal("error stat", stat)
		}
	}
}

func (self *helper) asyncPersistent(arg *persistentArg) {
	self.persistentCh <- arg
}

func (self *helper) DumpNode(node *radNode, level int, snapshot interface{}) error {
	if node == nil {
		return nil
	}

	self.getChildrenByNode(node, snapshot)

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
		self.DumpNode(n, level+1, snapshot)
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

func (self *helper) close() {
	close(self.reqch)
	close(self.persistentCh)
}
