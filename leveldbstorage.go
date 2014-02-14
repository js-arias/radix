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

	wo *leveldb.WriteOptions
}

var LAST_SEQ_KEY = []byte("##LAST_SEQ_KEY")

var l sync.Mutex

func (self *Levelstorage) Open(path string) (err error) {
	self.wo = leveldb.NewWriteOptions()
	self.opts = leveldb.NewOptions()
	self.cache = leveldb.NewLRUCache(1000 * 1024 * 1024)
	self.opts.SetCache(self.cache)

	self.opts.SetCreateIfMissing(true)
	self.opts.SetBlockSize(8 * 1024 * 1024)
	self.opts.SetWriteBufferSize(50 * 1024 * 1024)
	self.opts.SetCompression(leveldb.SnappyCompression)
	self.db, err = leveldb.Open(path, self.opts)

	return err
}

func (self *Levelstorage) BeginWriteBatch() error {
	l.Lock()
	if self.currentBatch != nil {
		logging.Fatal("writebatch already exist")
	}

	self.currentBatch = leveldb.NewWriteBatch()
	l.Unlock()
	return nil
}

func (self *Levelstorage) CommitWriteBatch() error {
	l.Lock()
	if self.currentBatch == nil {
		logging.Fatal("need to call BeginWriteBatch first")
	}
	err := self.db.Write(self.wo, self.currentBatch)
	self.currentBatch.Close()
	self.currentBatch = nil
	l.Unlock()
	return err
}

func (self *Levelstorage) Rollback() error {
	l.Lock()
	if self.currentBatch == nil {
		logging.Fatal("need to call BeginWriteBatch first")
	}
	self.currentBatch.Close()
	self.currentBatch = nil
	l.Unlock()
	return nil
}

func (self *Levelstorage) WriteNode(key string, value []byte) error {
	l.Lock()
	self.currentBatch.Put([]byte(key), value)
	l.Unlock()
	return nil
}

func (self *Levelstorage) ReadNode(key string, snapshot interface{}) ([]byte, error) {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}

	ro := leveldb.NewReadOptions()
	ro.SetFillCache(true)

	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}
	buf, err := self.db.Get(ro, []byte(key))
	ro.Close()
	return buf, err
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

	self.wo.Close()
	self.opts.Close()
	if self.cache != nil {
		self.cache.Close()
	}

	self.db = nil
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

func (self *Levelstorage) GetLastSeq(snapshot interface{}) (int64, error) {
	ro := leveldb.NewReadOptions()
	ro.SetFillCache(true)

	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}

	seqstr, err := self.db.Get(ro, LAST_SEQ_KEY)
	ro.Close()
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

func (self *Levelstorage) GetKey(key []byte, snapshot interface{}) ([]byte, error) {
	if len(key) == 0 {
		panic("key can't be nil")
		logging.Fatal("zero key found")
	}

	ro := leveldb.NewReadOptions()
	ro.SetFillCache(true)

	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}

	buf, err := self.db.Get(ro, key)
	ro.Close()

	return buf, err
}

func (self *Levelstorage) internalStats() string {
	property := self.db.PropertyValue("leveldb.stats")
	return property
}

func (self *Levelstorage) Stats() string {
	return self.dumpAll(nil) //self.internalStats()
}

func (self *Levelstorage) dumpAll(snapshot interface{}) string {
	b := bytes.Buffer{}
	b.WriteString("storage stats:\n")

	ro := leveldb.NewReadOptions()
	ro.SetFillCache(true)
	defer ro.Close()
	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}
	it := self.db.NewIterator(ro)
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
func (self *Levelstorage) IsEmpty(snapshot interface{}) bool {
	ro := leveldb.NewReadOptions()
	ro.SetFillCache(true)
	defer ro.Close()
	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}
	it := self.db.NewIterator(ro)
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

func (self *Levelstorage) Backup(path string, snapshot interface{}) error {
	ro := leveldb.NewReadOptions()
	defer ro.Close()
	if snapshot != nil {
		ro.SetSnapshot(snapshot.(*leveldb.Snapshot))
	}
	it := self.db.NewIterator(ro)
	defer it.Close()

	//create new database
	opts := leveldb.NewOptions()
	defer opts.Close()

	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(8 * 1024 * 1024)
	opts.SetWriteBufferSize(50 * 1024 * 1024)
	opts.SetCompression(leveldb.SnappyCompression)
	db, err := leveldb.Open(path, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	wo := leveldb.NewWriteOptions()
	defer wo.Close()

	cnt := 0
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		cnt++
		db.Put(wo, it.Key(), it.Value())
	}

	return nil
}

func (self *Levelstorage) NewSnapshot() interface{} {
	snapshot := self.db.NewSnapshot()
	if snapshot == nil {
		logging.Fatal("get snapshot failed")
	}

	return snapshot
}

func (self *Levelstorage) ReleaseSnapshot(snapshot interface{}) {
	if snapshot == nil {
		logging.Fatal("snapshot can't be nil")
	}

	self.db.ReleaseSnapshot(snapshot.(*leveldb.Snapshot))
}
