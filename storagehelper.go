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
	resultCh chan readResult //create less object
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
		n, err := self.readRadDiskNode(req.seq)
		if err != nil {
			logging.Fatalf("should never happend %+v", req)
			req.resultCh <- readResult{nil, err}
			continue
		}

		x := self.makeRadNode(n, req.seq)
		req.resultCh <- readResult{x, err}
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
	buf, err := enc.Marshal(x) //Marshal(x)
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
	// logging.Info("AddInMemoryNodeCount", n)
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
		return nil, fmt.Errorf("get key %d failed, is database empty?", seq)
	}

	var x radDiskNode
	err = enc.Unmarshal(buf, &x) //Unmarshal(buf, &x)
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

	// logging.Infof("%+v", tmp)
	if len(tmp.Children) > 0 {
		n.Children = make([]*radNode, len(tmp.Children), len(tmp.Children))
		self.AddInMemoryNodeCount(len(n.Children))
	} else {
		return nil
	}

	resultCh := make(chan readResult, len(tmp.Children))

	//send request
	for _, seq := range tmp.Children {
		self.reqch <- request{seq: seq, resultCh: resultCh}
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

	// logging.Infof("load from disk %+v", n)
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

func (self *helper) asyncPersistent(arg *persistentArg) {
	self.persistentCh <- arg
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

func (self *helper) close() {
	close(self.reqch)
	close(self.persistentCh)
}
