package radix

import (
	"bytes"
	leveldb "github.com/jmhodges/levigo"
	"github.com/ngaut/logging"
	"strconv"
)

type Levelstorage struct {
	currentBatch *leveldb.WriteBatch
	db           *leveldb.DB
	cache        *leveldb.Cache
	opts         *leveldb.Options
}

const LAST_SEQ_KEY = "##LAST_SEQ_KEY"

var (
	wo = leveldb.NewWriteOptions()
	ro = leveldb.NewReadOptions()
)

func (self *Levelstorage) Open(path string) (err error) {
	self.opts = leveldb.NewOptions()
	self.cache = leveldb.NewLRUCache(3 * 1024 * 1024 * 1024)
	self.opts.SetCache(self.cache)
	self.opts.SetCreateIfMissing(true)
	self.opts.SetBlockSize(4 * 1024 * 1024)
	self.opts.SetWriteBufferSize(50 * 1024 * 1024)
	// opts.SetCompression(leveldb.SnappyCompression)
	self.db, err = leveldb.Open(path, self.opts)
	ro.SetFillCache(true)
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
	err := self.db.Write(wo, self.currentBatch)
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
	self.currentBatch.Put([]byte(key), value)
	return nil
}

func (self *Levelstorage) ReadNode(key string) ([]byte, error) {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	return self.db.Get(ro, []byte(key))
}

func (self *Levelstorage) DelNode(key string) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	self.currentBatch.Delete([]byte(key))
	return nil
}

func (self *Levelstorage) Close() error {
	ro.Close()
	wo.Close()
	self.opts.Close()
	self.cache.Close()
	self.db.Close()
	return nil
}

func (self *Levelstorage) SaveLastSeq(seq int64) error {
	seqstr := strconv.FormatInt(seq, 10)
	self.currentBatch.Put([]byte(LAST_SEQ_KEY), []byte(seqstr))
	return nil
}

func (self *Levelstorage) GetLastSeq() (int64, error) {
	seqstr, err := self.db.Get(ro, []byte(LAST_SEQ_KEY))
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(string(seqstr), 10, 64)
}

func (self *Levelstorage) DeleteKey(key string) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	self.currentBatch.Delete([]byte(key))
	return nil
}

func (self *Levelstorage) PutKey(key string, value []byte) error {
	if len(key) == 0 {
		logging.Fatal("zero key found")
	}
	self.currentBatch.Put([]byte(key), value)
	return nil
}

func (self *Levelstorage) GetKey(key string) ([]byte, error) {
	if len(key) == 0 {
		panic("key can't be nil")
		logging.Fatal("zero key found")
	}
	return self.db.Get(ro, []byte(key))
}

func (self *Levelstorage) Stats() string {
	b := bytes.Buffer{}
	b.WriteString("storage stats:\n")
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
func (self *Levelstorage) IsEmpty() bool {
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
