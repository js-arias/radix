package radix

type Storage interface {
	Open(path string) error
	ReadNode(seq string) ([]byte, error)
	WriteNode(seq string, value interface{}) error
	DelNode(seq string) error
	DeleteKey(key string) error
	PutKey(key string, value []byte) error
	GetKey(key string) ([]byte, error)
	Close() error
	SaveLastSeq() error
	GetLastSeq() (int64, error)
	Stats() string
}
