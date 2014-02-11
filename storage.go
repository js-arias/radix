package radix

type Storage interface {
	Open(path string) error
	BeginWriteBatch() error
	CommitWriteBatch() error
	Rollback() error
	ReadNode(seq string, snapshot interface{}) ([]byte, error)
	WriteNode(seq string, value []byte) error
	DelNode(seq []byte) error
	DeleteKey(key []byte) error
	PutKey(key []byte, value []byte) error
	GetKey(key []byte, snapshot interface{}) ([]byte, error)
	Close() error
	SaveLastSeq(int64) error
	GetLastSeq(snapshot interface{}) (int64, error)
	Stats() string
	IsEmpty(snapshot interface{}) bool
	NewSnapshot() interface{}
	ReleaseSnapshot(snapshot interface{})
	Backup(path string, snapshot interface{}) error
}
