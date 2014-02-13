package radix

import (
	"container/list"
	enc "encoding/json"
	"fmt"
	"github.com/ngaut/logging"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Radix is a radix tree.
type Radix struct {
	Root                 *radNode     // Root of the radix tree
	lock                 sync.RWMutex // protect the radix
	path                 string
	MaxInMemoryNodeCount int64
	h                    *helper
	stats                Stats

	//for calc
	lastInsertNodeCnt int64

	tick    *time.Ticker
	closech chan bool

	snapshot interface{}
}

const (
	invalid_version = -1
)

type Stats struct {
	insertSuccess int64
	insertFailed  int64
	getSuccess    int64
	getFailed     int64
	cuts          int64
	lists         int64
	lastCheck     time.Time
}

func init() {
	logging.SetFlags(logging.Lshortfile | logging.LstdFlags)
	logging.SetLevelByString("debug")

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	rand.Seed(time.Now().UnixNano())

	go func() {
		logging.Info(http.ListenAndServe(":6060", nil))
	}()
}

// New returns a new, empty radix tree or open exist db.
func Open(path string) *Radix {
	logging.Info("open db")
	tree := &Radix{
		Root: &radNode{
			Seq: ROOT_SEQ, Stat: statOnDisk},
		path:    filepath.Join(path, "/db"),
		h:       NewHelper(&Levelstorage{}, ROOT_SEQ),
		closech: make(chan bool),
	}

	tree.lock.Lock()
	defer tree.lock.Unlock()

	if err := tree.h.store.Open(tree.path); err != nil {
		logging.Fatal(err)
	}

	// logging.Info(store.Stats())

	tree.beginWriteBatch()

	if err := tree.h.getChildrenByNode(tree.Root, tree.snapshot); err != nil {
		// logging.Info(err)
		tree.h.persistentNode(tree.Root, nil)
		tree.commitWriteBatch()
		logging.Infof("root: %+v", tree.Root)
	} else {
		tree.rollback()
		tree.Root.Stat = statInMemory
		logging.Debugf("root: %+v, last seq %d", tree.Root, tree.h.startSeq)
		tree.h.startSeq, err = tree.h.store.GetLastSeq(tree.snapshot)
		if err != nil || tree.h.startSeq < 0 {
			logging.Debug(tree.Stats())
			logging.Fatal(err, tree.h.startSeq)
		}
	}

	tree.MaxInMemoryNodeCount = 500000

	tree.tick = time.NewTicker(10 * time.Second)

	go tree.superVistor()

	return tree
}

func (self *Radix) calcSpeed() {
	if self.lastInsertNodeCnt == 0 {
		self.lastInsertNodeCnt = self.stats.insertSuccess
	}
	insertCnt := self.stats.insertSuccess - self.lastInsertNodeCnt
	sec := time.Since(self.stats.lastCheck).Seconds()
	if sec > 0 {
		logging.Debugf("%+v, speed %d, InMemoryNodeCount: %v", self.stats, int64(float64(insertCnt)/sec), self.h.GetInMemoryNodeCount())
	}

	self.stats.lastCheck = time.Now()
	self.lastInsertNodeCnt = self.stats.insertSuccess
}

func (self *Radix) superVistor() {
	for {
		select {
		case <-self.tick.C:
			self.lock.Lock()
			self.calcSpeed()
			self.addNodesCallBack()
			self.lock.Unlock()

			// logging.Debug("tick for checking nodes")
		case _, ok := <-self.closech:
			if !ok {
				return
			}
		}
	}
}

func (self *Radix) GetReadOnlyTree() *Radix {
	self.lock.Lock()
	defer self.lock.Unlock()
	tree := *self
	tree.snapshot = tree.h.store.NewSnapshot()
	r := *tree.Root
	r.Stat = statOnDisk
	r.Children = nil
	tree.Root = &r

	return &tree
}

func (self *Radix) addNodesCallBack() {
	count := self.h.GetInMemoryNodeCount()

	if count < self.MaxInMemoryNodeCount {
		return
	}

	self.stats.cuts++
	logging.Debug("need cutEdge", "current count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
	// logging.Debug("tree mem dump")
	// self.h.DumpMemNode(self.Root, 0)
	start := time.Now()
	if cutEdge(self.Root, self) == 0 {
		logging.Warning("cutEdge using", time.Since(start).Nanoseconds()/1000000000, "s", "count", count, "left", self.h.GetInMemoryNodeCount())
		return
	}
	logging.Debug("cutEdge using", time.Since(start).Nanoseconds()/1000000000, "s", "count", count, "left", self.h.GetInMemoryNodeCount())

	if self.h.GetInMemoryNodeCount() < 0 {
		panic("never happend")
	}

	// logging.Debug("after cut")
	// self.h.DumpMemNode(self.Root, 0)
	// logging.Info("left count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
}

func (self *Radix) cleanup() error {
	self.tick.Stop()
	close(self.closech)
	self.h.ResetInMemoryNodeCount()
	self.h.close()
	return self.h.store.Close()
}

func (self *Radix) Close() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	return self.cleanup()
}

func (self *Radix) Stats() string {
	return self.h.store.Stats()
}

func (self *Radix) Destory() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	logging.Warning("Destory!")
	self.cleanup()
	os.RemoveAll(self.path)
	return nil
}

func (self *Radix) DumpTree() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	logging.Info("dump tree:")
	if self.Root == nil {
		return nil
	}

	self.h.DumpNode(self.Root, 0, self.snapshot)

	return nil
}

func (self *Radix) DumpMemTree() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	logging.Info("dump mem tree:")

	if self.Root == nil {
		return nil
	}

	self.h.DumpMemNode(self.Root, 0)
	return nil
}

// Delete removes the Value associated with a particular key and returns it.
func (self *Radix) Delete(key string) []byte {
	logging.Info("delete", key)
	k := []byte(key)
	self.lock.Lock()
	self.beginWriteBatch()
	b := self.Root.delete(k, self)
	err := self.commitWriteBatch()
	if err != nil {
		logging.Fatal(err)
		self.lock.Unlock()
		return nil
	}

	self.lock.Unlock()
	return b
}

// Insert put a Value in the radix. It returns old value if exist
func (self *Radix) Insert(key string, Value string, version int64) ([]byte, error) {
	start := time.Now()
	defer func() {
		if n := time.Since(start).Nanoseconds() / 1000 / 1000; n > 500 {
			logging.Warning("too slow insert using", n, "milsec")
		}
	}()

	k := []byte(key)
	vv := &versionValue{Version: version, Value: Value}
	internalKey := encodeValueToInternalKey(k)

	buf, err := enc.Marshal(vv)
	if err != nil {
		logging.Error(err)
		return nil, err
	}

	self.lock.Lock()

	self.beginWriteBatch()
	oldvalue, err := self.Root.put(k, buf, internalKey, invalid_version, false, self)
	if err != nil {
		self.stats.insertFailed++
		logging.Info(err)
		self.commitWriteBatch()
		self.lock.Unlock()
		return nil, err
	}

	err = self.commitWriteBatch()
	if err != nil {
		self.stats.insertFailed++
		logging.Fatal(err)
		self.lock.Unlock()
		return nil, err
	}

	self.stats.insertSuccess++

	self.lock.Unlock()
	return oldvalue, nil
}

func (self *Radix) CAS(key string, Value string, version int64, newVersion int64) ([]byte, error) {
	k := []byte(key)
	vv := &versionValue{Version: newVersion, Value: Value}
	internalKey := encodeValueToInternalKey(k)

	buf, err := enc.Marshal(vv)
	if err != nil {
		logging.Error(err)
		return nil, err
	}

	self.lock.Lock()
	defer func() {
		// self.addNodesCallBack()
		self.lock.Unlock()
	}()

	self.beginWriteBatch()
	oldvalue, err := self.Root.put(k, buf, internalKey, version, false, self)
	if err != nil {
		logging.Info(err)
		self.commitWriteBatch()
		return nil, err
	}

	err = self.commitWriteBatch()
	if err != nil {
		logging.Fatal(err)
		return nil, err
	}

	return oldvalue, nil
}

// Lookup searches for a particular string in the tree.
func (self *Radix) Lookup(key string) []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()

	if x, _, ok := self.Root.lookup([]byte(key), self); ok && len(x.Value) > 0 {
		// logging.Debugf("GetValueFromStore %+v", x)
		vv, err := self.h.GetValueFromStore(x.Value, self.snapshot)
		if err != nil {
			return nil
		}
		return []byte(vv.Value)
	}

	return nil
}

func (self *Radix) GetFirstLevelChildrenCount(key string) int {
	k := []byte(key)
	self.lock.RLock()
	defer self.lock.RUnlock()

	if x, _, _ := self.Root.lookup(k, self); x != nil {
		return len(x.Children)
	}

	//means not found
	return -1
}

func (self *Radix) FindInternalKey(key string) string {
	k := []byte(key)
	self.lock.RLock()
	defer self.lock.RUnlock()

	if x, _, _ := self.Root.lookup(k, self); x != nil {
		return string(x.Value)
	}

	return ""
}

// Lookup searches for a particular string in the tree.
func (self *Radix) GetWithVersion(key string) ([]byte, int64) {
	k := []byte(key)
	self.lock.RLock()
	//todo: using internal key to get, no need to call lookup
	if x, _, ok := self.Root.lookup(k, self); ok && len(x.Value) > 0 {
		vv, err := self.h.GetValueFromStore(x.Value, self.snapshot)
		if err != nil {
			atomic.AddInt64(&self.stats.getFailed, 1)
			self.lock.RUnlock()
			return nil, -1
		}

		atomic.AddInt64(&self.stats.getSuccess, 1)

		self.lock.RUnlock()
		return []byte(vv.Value), vv.Version
	}

	atomic.AddInt64(&self.stats.getFailed, 1)
	self.lock.RUnlock()
	return nil, -1
}

func (self *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, limitCount int32, limitLevel int, marker string) *list.List {
	key := []byte(prefix)

	logging.Info("limitCount", limitCount, "prefix", prefix, "marker", marker)

	if len(marker) > 0 {
		key = []byte(marker)
	}

	delim := []byte(delimiter)

	self.lock.RLock()
	defer self.lock.RUnlock()

	node, _, exist := self.Root.lookup(key, self)
	if node == nil {
		return list.New()
	}
	logging.Info(node.Prefix, "---", node.Value)

	var skipRoot bool
	if exist && len(marker) > 0 {
		skipRoot = true
	}

	var currentCount int32

	l := list.New()
	node.listByPrefixDelimiterMarker(skipRoot, delim, limitCount, limitLevel, &currentCount, self, l)
	for e := l.Front(); e != nil; e = e.Next() {
		tuple := e.Value.(*Tuple)
		key := tuple.Value
		if tuple.Type == RESULT_CONTENT {
			value, err := self.h.store.GetKey([]byte(key), self.snapshot)
			if err != nil {
				logging.Error("should never happend", e.Value)
				continue
			}
			tuple.Value = string(value)
			e.Value = tuple
		}
	}

	return l
}

// Prefix returns a list of elements that share a given prefix.
func (self *Radix) Prefix(prefix string) *list.List {
	pre := []byte(prefix)

	self.lock.RLock()
	defer self.lock.RUnlock()

	l := list.New()
	n, _, _ := self.Root.lookup(pre, self)
	if n == nil {
		return l
	}
	// logging.Info("now add to list")
	n.addToList(l, self)
	return l
}

func (self *Radix) SetMaxInMemoryNodeCount(count int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	self.MaxInMemoryNodeCount = count
}

func (self *Radix) StoragePut(key, value []byte) error {
	if len(key) == 0 || len(value) == 0 {
		return fmt.Errorf("key and value can't be nil: %s-%s", string(key), string(value))
	}

	if key[0] != '*' {
		return fmt.Errorf("key should start with *", string(key))
	}

	self.lock.RLock()
	defer self.lock.RUnlock()

	self.h.store.BeginWriteBatch()

	err := self.h.store.PutKey(key, value)
	if err != nil {
		self.h.store.Rollback()
		return err
	}

	return self.h.store.CommitWriteBatch()
}

func (self *Radix) StorageGet(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key and value can't be nil: %s", string(key))
	}

	if key[0] != '*' {
		return nil, fmt.Errorf("key should start with *", string(key))
	}

	self.lock.RLock()
	defer self.lock.RUnlock()

	return self.h.store.GetKey(key, self.snapshot)
}

func (self *Radix) Backup(path string) chan error {
	rtree := self.GetReadOnlyTree()
	ch := make(chan error, 1)
	go func() {
		ch <- rtree.h.store.Backup(path, rtree.snapshot)
		rtree.h.store.ReleaseSnapshot(rtree.snapshot)
	}()

	return ch
}
