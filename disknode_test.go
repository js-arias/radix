package radix

import (
	"bytes"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	n := radDiskNode{Prefix: "prefixn", Value: "x", Version: 1, Seq: 2}
	n1 := radDiskNode{Prefix: "prefixn1", Value: "x1", Version: 11, Seq: 22}
	n2 := radDiskNode{Prefix: "prefixn2", Value: "x2", Version: 12, Seq: 24}
	n.Children = make([]int64, 2, 2)
	n.Children[0] = n1.Seq
	n.Children[1] = n2.Seq

	b := &bytes.Buffer{}
	en := NewradDiskNodeJSONEncoder(b)
	en.Encode(&n)

	println(b.String())

	de := NewradDiskNodeJSONDecoder(b)
	var x *radDiskNode
	if err := de.Decode(&x); err != nil {
		t.Error(err)
		return
	}

	if len(x.Children) != 2 || x.Children[0] != 22 || x.Children[1] != 24 {
		t.Error("decode children failed")
	}
}
