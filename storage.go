package radix

type Storage interface {
	Open(path string) error
	WriteNode(key string, value interface{}) error
	ReadNode(key string) ([]byte, error)
	Close() error
	SaveLastSeq() error
	GetLastSeq() (int64, error)
}
