package radix

import (
	"container/list"
	"github.com/ngaut/logging"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Radix is a radix tree.
type Radix struct {
	Root                 *radNode     // Root of the radix tree
	lock                 sync.RWMutex // protect the radix
	path                 string
	MaxInMemoryNodeCount int64
	h                    *helper
}

const (
	invalid_version = -1
)

func init() {
	logging.SetFlags(logging.Lshortfile | logging.LstdFlags)
	logging.SetLevelByString("debug")
}

// New returns a new, empty radix tree.
func New(path string) *Radix {
	logging.Info("open db")
	rad := &Radix{
		Root: &radNode{
			Seq: ROOT_SEQ, InDisk: true},
		path: filepath.Join(path, "/db"),
		h:    &helper{store: &Levelstorage{}, startSeq: ROOT_SEQ},
	}

	rad.lock.Lock()
	defer rad.lock.Unlock()

	if err := rad.h.store.Open(rad.path); err != nil {
		logging.Fatal(err)
	}

	// logging.Info(store.Stats())

	rad.beginWriteBatch()

	if err := rad.h.getChildrenByNode(rad.Root); err != nil {
		// logging.Info(err)
		rad.h.persistentNode(*rad.Root, nil)
		rad.commitWriteBatch()
		logging.Infof("root: %+v", rad.Root)
	} else {
		rad.rollback()
		rad.Root.InDisk = false
		logging.Infof("root: %+v", rad.Root)
		_, err = rad.h.store.GetLastSeq()
		if err != nil {
			logging.Fatal(err)
		}
	}

	rad.MaxInMemoryNodeCount = 1000

	return rad
}

func (self *Radix) addCallBack() {
	if self.h.GetInMemoryNodeCount() > self.MaxInMemoryNodeCount {
		// logging.Info("need cutEdge", "current count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
		// logging.Info("tree mem dump")
		// self.h.DumpMemNode(self.Root, 0)

		cutEdge(self.Root, self)
		// logging.Infof("%+v", self.Root)
		// logging.Info("left count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
	}
}

func (self *Radix) cleanup() error {
	self.h.ResetInMemoryNodeCount()
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

	logging.Info("Destory!")
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

	self.h.DumpNode(self.Root, 0)

	return nil
}

func (self *Radix) DumpMemTree() {
	self.lock.Lock()
	defer self.lock.Unlock()

	logging.Info("dump mem tree:")

	if self.Root == nil {
		return
	}

	self.h.DumpMemNode(self.Root, 0)
}

// Delete removes the Value associated with a particular key and returns it.
//todo: using transaction
func (self *Radix) Delete(key string) []byte {
	self.lock.Lock()
	defer self.lock.Unlock()

	// logging.Info("delete", key)
	self.beginWriteBatch()
	b := self.Root.delete(key, self)
	err := self.commitWriteBatch()
	if err != nil {
		logging.Fatal(err)
		return nil
	}

	return b
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
func (self *Radix) Insert(key string, Value string) ([]byte, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	start := time.Now()
	defer func() {
		if n := time.Since(start).Nanoseconds() / 1000 / 1000; n > 100 {
			logging.Info("too slow insert using", n, "milsec")
		}
	}()

	self.beginWriteBatch()
	oldvalue, err := self.Root.put(key, []byte(Value), key, invalid_version, false, self)
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

	self.addCallBack()

	return oldvalue, nil
}

func (self *Radix) CAS(key string, Value string, version int64) ([]byte, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.beginWriteBatch()
	oldvalue, err := self.Root.put(key, []byte(Value), key, version, false, self)
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

	self.addCallBack()

	return oldvalue, nil
}

// Lookup searches for a particular string in the tree.
func (self *Radix) Lookup(key string) []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()

	if x, _, ok := self.Root.lookup(key, self); ok {
		buf, err := self.h.GetValueFromStore(x.Value)
		if err != nil {
			return nil
		}
		return buf
	}

	self.addCallBack()

	return nil
}

// Lookup searches for a particular string in the tree.
func (self *Radix) GetWithVersion(key string) ([]byte, int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	if x, _, ok := self.Root.lookup(key, self); ok {
		buf, err := self.h.GetValueFromStore(x.Value)
		if err != nil {
			return nil, 0
		}
		return buf, x.Version
	}

	self.addCallBack()

	return nil, 0
}

func (self *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, limitCount int32, limitLevel int, marker string) *list.List {
	self.lock.RLock()
	defer self.lock.RUnlock()

	logging.Info("limitCount", limitCount)

	node, _, _ := self.Root.lookup(prefix, self)
	if node == nil {
		return list.New()
	}
	// println(node.Prefix, "---", node.Value)

	var currentCount int32

	l := node.getFirstByDelimiter(marker, delimiter, limitCount, limitLevel, &currentCount, self)
	self.addCallBack()

	return l
}

// Prefix returns a list of elements that share a given prefix.
func (self *Radix) Prefix(prefix string) *list.List {
	self.lock.RLock()
	defer self.lock.RUnlock()

	l := list.New()
	n, _, _ := self.Root.lookup(prefix, self)
	if n == nil {
		return l
	}
	logging.Info("now add to list")
	n.addToList(l, self)
	self.addCallBack()
	return l
}

func (self *Radix) SetMaxInMemoryNodeCount(count int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	self.MaxInMemoryNodeCount = count
}
