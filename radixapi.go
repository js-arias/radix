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
	Root *radNode     // Root of the radix tree
	lock sync.RWMutex // protect the radix
	path string
}

const (
	invalid_version = -1
)

// New returns a new, empty radix tree.
func New(path string) *Radix {
	log.Println("open db")
	rad := &Radix{
		Root: &radNode{Seq: ROOT_SEQ, InDisk: true},
		path: path + "/db",
	}

	rad.lock.Lock()
	defer rad.lock.Unlock()

	if err := store.Open(rad.path); err != nil {
		log.Fatal(err)
	}

	// log.Println(store.Stats())

	rad.beginWriteBatch()

	if err := getChildrenByNode(rad.Root); err != nil {
		// log.Println(err)
		persistentNode(*rad.Root, nil)
		rad.commitWriteBatch()
		log.Printf("root: %+v", rad.Root)
	} else {
		rad.rollback()
		rad.Root.InDisk = false
		log.Printf("root: %+v", rad.Root)
		startSeq, err = store.GetLastSeq()
		if err != nil {
			log.Fatal(err)
		}
	}

	return rad
}

func (rad *Radix) Close() error {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	log.Println("close db")
	return store.Close()
}

func (rad *Radix) Stats() string {
	return store.Stats()
}

func (rad *Radix) Destory() error {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	log.Println("Destory!")
	store.Close()
	os.RemoveAll(rad.path)
	return nil
}

func (rad *Radix) DumpTree() error {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	if rad.Root == nil {
		return nil
	}

	log.Println("dump tree:")

	DumpNode(rad.Root, 0)

	return nil
}

// Delete removes the Value associated with a particular key and returns it.
//todo: using transaction
func (rad *Radix) Delete(key string) []byte {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	// log.Println("delete", key)
	rad.beginWriteBatch()
	b := rad.Root.delete(key)
	err := rad.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
		return nil
	}

	return b
}

// Insert put a Value in the radix. It returns an error if the given key
// is already in use.
//todo: using transaction(batch write)
func (rad *Radix) Insert(key string, Value string) ([]byte, error) {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	start := time.Now()
	defer func() {
		if n := time.Since(start).Nanoseconds() / 1000 / 1000; n > 100 {
			log.Println("too slow insert using", n, "milsec")
		}
	}()

	rad.beginWriteBatch()
	oldvalue, err := rad.Root.put(key, []byte(Value), key, invalid_version, false)
	if err != nil {
		log.Println(err)
		rad.commitWriteBatch()
		return nil, err
	}

	err = rad.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return oldvalue, nil
}

func (rad *Radix) CAS(key string, Value string, version int64) ([]byte, error) {
	rad.lock.Lock()
	defer rad.lock.Unlock()

	rad.beginWriteBatch()
	oldvalue, err := rad.Root.put(key, []byte(Value), key, version, false)
	if err != nil {
		log.Println(err)
		rad.commitWriteBatch()
		return nil, err
	}

	err = rad.commitWriteBatch()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return oldvalue, nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) Lookup(key string) []byte {
	rad.lock.RLock()
	defer rad.lock.RUnlock()

	if x, _, ok := rad.Root.lookup(key); ok {
		buf, err := GetValueFromStore(x.Value)
		if err != nil {
			return nil
		}
		return buf
	}

	return nil
}

// Lookup searches for a particular string in the tree.
func (rad *Radix) GetWithVersion(key string) ([]byte, int64) {
	rad.lock.RLock()
	defer rad.lock.RUnlock()

	if x, _, ok := rad.Root.lookup(key); ok {
		buf, err := GetValueFromStore(x.Value)
		if err != nil {
			return nil, 0
		}
		return buf, x.Version
	}

	return nil, 0
}

//todo: support marker & remove duplicate, see TestLookupByPrefixAndDelimiter_complex
func (rad *Radix) LookupByPrefixAndDelimiter(prefix string, delimiter string, limitCount int32, limitLevel int, marker string) *list.List {
	rad.lock.RLock()
	defer rad.lock.RUnlock()

	println("limitCount", limitCount)

	node, _, _ := rad.Root.lookup(prefix)
	if node == nil {
		return list.New()
	}
	// println(node.Prefix, "---", node.Value)

	var currentCount int32

	return node.getFirstByDelimiter(marker, delimiter, limitCount, limitLevel, &currentCount)
}

// Prefix returns a list of elements that share a given prefix.
func (rad *Radix) Prefix(prefix string) *list.List {
	rad.lock.RLock()
	defer rad.lock.RUnlock()

	l := list.New()
	n, _, _ := rad.Root.lookup(prefix)
	if n == nil {
		return l
	}
	n.addToList(l)
	return l
}
