package radix

import (
	leveldb "github.com/jmhodges/levigo"
	"log"
	"strconv"
)

type Levelstorage struct {
	db *leveldb.DB
}

const LAST_SEQ_KEY = "##LAST_SEQ_KEY"

var (
	wo = leveldb.NewWriteOptions()
	ro = leveldb.NewReadOptions()
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func (self *Levelstorage) Open(path string) (err error) {
	opts := leveldb.NewOptions()
	opts.SetCache(leveldb.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(4 * 1024 * 1024)
	opts.SetWriteBufferSize(50 * 1024 * 1024)
	self.db, err = leveldb.Open(path, opts)
	return err
}

func (self *Levelstorage) WriteNode(key string, value []byte) error {
	return self.db.Put(wo, []byte(key), value)
}

func (self *Levelstorage) ReadNode(key string) ([]byte, error) {
	return self.db.Get(ro, []byte(key))
}

func (self *Levelstorage) Close() error {
	self.db.Close()
	return nil
}

func (self *Levelstorage) SaveLastSeq(seq int64) error {
	seqstr := strconv.FormatInt(seq, 10)
	return self.db.Put(wo, []byte(LAST_SEQ_KEY), []byte(seqstr))
}

func (self *Levelstorage) GetLastSeq() (int64, error) {
	seqstr, err := self.db.Get(ro, []byte(LAST_SEQ_KEY))
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(string(seqstr), 10, 64)
}
