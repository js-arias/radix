package radix

import (
	"container/list"
	"log"
	"os"
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

// New returns a new, empty radix tree.
func New(path string) *Radix {
	log.Println("open db")
	rad := &Radix{
		Root: &radNode{
			Seq: ROOT_SEQ, InDisk: true},
		path: path + "/db",
		h:    &helper{store: &Levelstorage{}, startSeq: ROOT_SEQ},
	}

	rad.lock.Lock()
	defer rad.lock.Unlock()

	if err := rad.h.store.Open(rad.path); err != nil {
		log.Fatal(err)
	}

	// log.Println(store.Stats())

	rad.beginWriteBatch()

	if err := rad.h.getChildrenByNode(rad.Root); err != nil {
		// log.Println(err)
		rad.h.persistentNode(*rad.Root, nil)
		rad.commitWriteBatch()
		log.Printf("root: %+v", rad.Root)
	} else {
		rad.rollback()
		rad.Root.InDisk = false
		log.Printf("root: %+v", rad.Root)
		_, err = rad.h.store.GetLastSeq()
		if err != nil {
			log.Fatal(err)
		}
	}

	return rad
}

func (self *Radix) addCallBack() {
	if self.h.GetInMemoryNodeCount() > self.MaxInMemoryNodeCount {
		log.Println("need cutEdge", "current count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
		log.Println("tree mem dump")
		self.h.DumpMemNode(self.Root, 0)

		cutEdge(self.Root, self)
		log.Printf("%+v", self.Root)
		log.Println("left count", self.h.GetInMemoryNodeCount(), "MaxInMemoryNodeCount", self.MaxInMemoryNodeCount)
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

	log.Println("Destory!")
	self.cleanup()
	os.RemoveAll(self.path)
	return nil
}

func (self *Radix) DumpTree() error {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Println("dump tree:")
	if self.Root == nil {
		return nil
	}

	self.h.DumpNode(self.Root, 0)

	return nil
}

func (self *Radix) DumpMemTree() {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Println("dump mem tree:")

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

	// log.Println("delete", key)
	self.beginWriteBatch()
	b := self.Root.delete(key, self)
	err := self.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
		return nil
	}

	return b
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
//todo: using transaction(batch write)
func (self *Radix) Insert(key string, Value string) ([]byte, error) {
	self.lock.Lock()
	defer self.lock.Unlock()

	start := time.Now()
	defer func() {
		if n := time.Since(start).Nanoseconds() / 1000 / 1000; n > 100 {
			log.Println("too slow insert using", n, "milsec")
		}
	}()

	self.beginWriteBatch()
	oldvalue, err := self.Root.put(key, []byte(Value), key, invalid_version, false, self)
	if err != nil {
		log.Println(err)
		self.commitWriteBatch()
		return nil, err
	}

	err = self.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
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
		log.Println(err)
		self.commitWriteBatch()
		return nil, err
	}

	err = self.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
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

//todo: support marker & remove duplicate, see TestLookupByPrefixAndDelimiter_complex
func (self *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, limitCount int32, limitLevel int, marker string) *list.List {
	self.lock.RLock()
	defer self.lock.RUnlock()

	println("limitCount", limitCount)

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
	n.addToList(l)
	self.addCallBack()
	return l
}

func (self *Radix) SetMaxInMemoryNodeCount(count int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()

	self.MaxInMemoryNodeCount = count
}
