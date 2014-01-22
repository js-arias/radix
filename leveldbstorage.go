package radix

import (
	"bytes"
	"fmt"
	leveldb "github.com/jmhodges/levigo"
	"github.com/ngaut/logging"
	"strconv"
	"sync"
)

type Levelstorage struct {
	currentBatch *leveldb.WriteBatch
	db           *leveldb.DB
	cache        *leveldb.Cache
	opts         *leveldb.Options
	ro           *leveldb.ReadOptions
	wo           *leveldb.WriteOptions
}

var LAST_SEQ_KEY = []byte("##LAST_SEQ_KEY")

var l sync.Mutex

func (self *Levelstorage) Open(path string) (err error) {
	self.ro = leveldb.NewReadOptions()
	self.wo = leveldb.NewWriteOptions()
	self.opts = leveldb.NewOptions()
	self.cache = leveldb.NewLRUCache(1 * 1024 * 1024 * 1024)
	self.opts.SetCache(self.cache)
	self.ro.SetFillCache(true)

	self.opts.SetCreateIfMissing(true)
	self.opts.SetBlockSize(8 * 1024 * 1024)
	self.opts.SetWriteBufferSize(50 * 1024 * 1024)
	self.opts.SetCompression(leveldb.SnappyCompression)
	self.db, err = leveldb.Open(path, self.opts)

	return err
}

func (self *Levelstorage) BeginWriteBatch() error {
	if self.currentBatch != nil {
		logging.Fatal("writebatch already exist")
	}

	self.currentBatch = leveldb.NewWriteBatch()
	return nil
}

func (self *Levelstorage) CommitWriteBatch() error {
	if self.currentBatch == nil {
		logging.Fatal("need to call BeginWriteBatch first")
	}
	err := self.db.Write(self.wo, self.currentBatch)
	self.currentBatch.Close()
	self.currentBatch = nil
	return err
}

func (self *Levelstorage) Rollback() error {
	if self.currentBatch == nil {
		logging.Fatal("need to call BeginWriteBatch first")
	}
	self.currentBatch.Close()
	self.currentBatch = nil
	return nil
}

func (self *Levelstorage) WriteNode(key string, value []byte) error {
	l.Lock()
	self.currentBatch.Put([]byte(key), value)
	l.Unlock()
	return nil
}

func (self *Levelstorage) ReadNode(key string) ([]byte, error) {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}

	return self.db.Get(self.ro, []byte(key))
}

func (self *Levelstorage) DelNode(key []byte) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	l.Lock()
	self.currentBatch.Delete(key)
	l.Unlock()
	return nil
}

func (self *Levelstorage) Close() error {
	self.db.Close()
	self.ro.Close()
	self.wo.Close()
	self.opts.Close()
	if self.cache != nil {
		self.cache.Close()
	}

	self.db = nil
	self.ro = nil
	self.wo = nil
	self.opts = nil
	self.cache = nil
	return nil
}

func (self *Levelstorage) SaveLastSeq(seq int64) error {
	seqstr := strconv.FormatInt(seq, 10)
	l.Lock()
	self.currentBatch.Put(LAST_SEQ_KEY, []byte(seqstr))
	l.Unlock()
	return nil
}

func (self *Levelstorage) GetLastSeq() (int64, error) {
	seqstr, err := self.db.Get(self.ro, LAST_SEQ_KEY)
	if err != nil {
		return -1, err
	}

	if seqstr == nil {
		return -1, fmt.Errorf("%s doesn't exist", string(LAST_SEQ_KEY))
	}

	return strconv.ParseInt(string(seqstr), 10, 64)
}

func (self *Levelstorage) DeleteKey(key []byte) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	l.Lock()
	self.currentBatch.Delete(key)
	l.Unlock()
	return nil
}

func (self *Levelstorage) PutKey(key []byte, value []byte) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	l.Lock()
	self.currentBatch.Put(key, value)
	l.Unlock()
	return nil
}

func (self *Levelstorage) GetKey(key []byte) ([]byte, error) {
	if len(key) == 0 {
		panic("key can't be nil")
		logging.Fatal("zero key found")
	}
	return self.db.Get(self.ro, key)
}

func (self *Levelstorage) internalStats() string {
	property := self.db.PropertyValue("leveldb.stats")
	return property
}

func (self *Levelstorage) Stats() string {
	return self.dumpAll() //self.internalStats()
}

func (self *Levelstorage) dumpAll() string {
	b := bytes.Buffer{}
	b.WriteString("storage stats:\n")
	it := self.db.NewIterator(self.ro)
	defer it.Close()

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		b.WriteString(string(it.Key()))
		b.WriteString("  ----> ")
		b.WriteString(string(it.Value()))
		b.WriteString("\n")
	}

	return b.String()
}

// ##LAST_SEQ_KEY  ----> 19
// -1  ----> {"Version":0,"Seq":-1,"OnDisk":true}
func (self *Levelstorage) IsEmpty() bool {
	it := self.db.NewIterator(self.ro)
	defer it.Close()

	cnt := 0

	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		cnt++
		if cnt > 2 {
			return false
		}
	}

	return true
}
