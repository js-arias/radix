package radix

type Storage interface {
	Open(path string) error
	BeginWriteBatch() error
	CommitWriteBatch() error
	Rollback() error
	ReadNode(seq string) ([]byte, error)
	WriteNode(seq string, value []byte) error
	DelNode(seq []byte) error
	DeleteKey(key []byte) error
	PutKey(key []byte, value []byte) error
	GetKey(key []byte) ([]byte, error)
	Close() error
	SaveLastSeq(int64) error
	GetLastSeq() (int64, error)
	Stats() string
	IsEmpty() bool
}
