package radix

import (
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"strconv"
)

type Levelstorage struct {
	db *leveldb.DB
}

const LAST_SEQ_KEY = "##LAST_SEQ_KEY"

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

func (self *Levelstorage) Open(path string) (err error) {
	self.db, err = leveldb.OpenFile(path, nil)
	return err
}

func (self *Levelstorage) WriteNode(key string, value []byte) error {
	return self.db.Put([]byte(key), value, nil)
}

func (self *Levelstorage) ReadNode(key string) ([]byte, error) {
	return self.db.Get([]byte(key), nil)
}

func (self *Levelstorage) Close() error {
	return self.db.Close()
}

func (self *Levelstorage) SaveLastSeq(seq int64) error {
	seqstr := strconv.FormatInt(seq, 10)
	return self.db.Put([]byte(LAST_SEQ_KEY), []byte(seqstr), nil)
}

func (self *Levelstorage) GetLastSeq() (int64, error) {
	seqstr, err := self.db.Get([]byte(LAST_SEQ_KEY), nil)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(string(seqstr), 10, 64)
}
