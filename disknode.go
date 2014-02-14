package radix

type radDiskNode struct {
	Prefix string `json:"p,omitempty"`   // current prefix of the node
	Value  string `json:"val,omitempty"` // stored key
	// Seq      int64   `json:"seq"`
	Children []int64 `json:"c,omitempty"`
}
