// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package radix

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

const COUNT = 1000000

func TestInsertion(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	for _, d := range r.Root.Children {
		if v := d.Value.(string); v != d.Prefix {
			t.Errorf("d.Value = %s, want %s", v, d.Prefix)
		}
	}
	r.Insert("slower", "slower")
	for _, d := range r.Root.Children {
		if d.Prefix != "slow" {
			continue
		}
		if v := d.Value.(string); v != "slow" {
			t.Errorf("d.Value = %s, want %s", v, d.Prefix)
		}
		if d.Children[0].Prefix == "er" {
			if v := d.Children[0].Value.(string); v != "slower" {
				t.Errorf("d.Children.Value = %s, want %s", v, "slower")
			}
			break
		}
		t.Errorf("d.Children.prefix = %s, want %s", d.Children[0].Prefix, "er")
	}
	r.Insert("team", "team")
	r.Insert("tester", "tester")
	var ok bool
	for _, d := range r.Root.Children {
		if d.Prefix == "te" {
			if v, ok := d.Value.(string); ok {
				t.Errorf("d.Value = %s, want nil", v)
			}
			ok = true
			for _, n := range d.Children {
				switch v := n.Value.(string); n.Prefix {
				case "am":
					if v != "team" {
						t.Errorf("n.Value = %s, want %s", v, "team")
					}
				case "st":
					if v != "test" {
						t.Errorf("n.Value = %s, want %s", v, "test")
					}
					if n.Children == nil {
						t.Errorf("nil Value unexpected in n.Children")
					}
				default:
					t.Errorf("n.Value = %s, want %s or %s", v, "team", "tester")
				}
			}
			break
		}
	}
	if !ok {
		t.Errorf("expecting te prefix, not found")
	}
	r.Insert("te", "te")
	ok = false
	for _, d := range r.Root.Children {
		if d.Prefix == "te" {
			v := d.Value.(string)
			if v != "te" {
				t.Errorf("d.Value = %s, want %s", v, "te")
			}
			ok = true
			break
		}
	}
	if !ok {
		t.Errorf("expecting te prefix, not found")
	}
	if r.Insert("slow", "slow") == nil {
		t.Errorf("expecting error at insert")
	}
	if r.Insert("water", "water") == nil {
		t.Errorf("expecting error at insert")
	}
	if r.Insert("team", "team") == nil {
		t.Errorf("expecting error at insert")
	}
}

func TestLookupByPrefixAndDelimiter(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "")
	r.Insert("slow", "")
	r.Insert("water", "")
	r.Insert("slower", "")
	r.Insert("tester", "")
	r.Insert("team", "")
	r.Insert("toast", "")
	r.Insert("te", "te")
	r.Insert("test123/1", "")
	r.Insert("test123/2", "")
	r.Insert("test123//2", "")

	l := r.LookupByPrefixAndDelimiter("t", "/", 100, 100, "")
	if l.Len() != 6 {
		t.Errorf("should got 5, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
}

func TestLookupByPrefixAndDelimiter_complex(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("te#st", "")
	r.Insert("slow", "")
	r.Insert("water", "")
	r.Insert("slower", "")
	r.Insert("tester", "")
	r.Insert("team", "")
	r.Insert("toast", "")
	r.Insert("te", "te")
	r.Insert("test123/1//a", "")
	r.Insert("test123/2", "")
	r.Insert("test123//2", "")

	l := r.LookupByPrefixAndDelimiter("t", "#", 100, 100, "")
	if l.Len() != 8 {
		t.Errorf("should got 8, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
}

func TestLookupByPrefixAndDelimiter_limit(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "")
	r.Insert("slow", "")
	r.Insert("water", "")
	r.Insert("slower", "")
	r.Insert("tester", "")
	r.Insert("team", "")
	r.Insert("toast", "")
	r.Insert("te", "te")
	r.Insert("test123/1", "")
	r.Insert("test123/2", "")
	r.Insert("test123//2", "")

	l := r.LookupByPrefixAndDelimiter("t", "/", 2, 100, "")
	if l.Len() != 2 {
		t.Errorf("should got 2, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
}

func TestLookupByPrefixAndDelimiter_limit_marker(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "")
	r.Insert("slow", "")
	r.Insert("water", "")
	r.Insert("slower", "")
	r.Insert("tester", "")
	r.Insert("team", "")
	r.Insert("toast", "")
	r.Insert("te", "te")
	r.Insert("test123/1", "")
	r.Insert("test123/2", "")
	r.Insert("test123//2", "")

	println("---------------------------")
	l := r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "test")
	if l.Len() != 3 {
		t.Errorf("should got 3, but we got %d", l.Len())
		println("================================")
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
		println("---------------------------")
	}

	l = r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "te")
	if l.Len() != 5 {
		t.Errorf("should got 5, but we got %d", l.Len())
		println("================================")
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
		println("---------------------------")
	}
}

func TestLookupByPrefixAndDelimiter_complex_many(t *testing.T) {
	r := New(".")

	start := time.Now()
	for i := 0; i < COUNT; i++ {
		key := fmt.Sprintf("2013/%d", i)
		r.Insert(key, "")
	}
	println("Insert", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")
	r.Close()

	r = New(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	println("lookup", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	start = time.Now()
	_, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
	println("json marshal using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	start = time.Now()
	var buffer bytes.Buffer
	err = gob.NewEncoder(&buffer).Encode(r)
	if err != nil {
		t.Fatal(err)
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
	println("gob marshal using:", time.Since(start).Nanoseconds()/1000000000, " sec")
	r.Close()

	r = New(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", COUNT/10, 10, "2013/1")
	if l.Len() != COUNT/10 {
		t.Errorf("should got %d, but we got %d", COUNT/10, l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}

	println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
}

func TestLookupByPrefixAndDelimiter_complex_many_bigkey(t *testing.T) {
	r := New(".")

	start := time.Now()
	b := bytes.Buffer{}
	for i := 0; i < 1000; i++ {
		b.WriteByte('c')
	}

	buf := b.String()
	for i := 0; i < COUNT; i++ {
		key := fmt.Sprintf("2013/%d", i)
		r.Insert(key+buf, b.Bytes())
	}

	r.Close()

	println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$big key Insert", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r = New(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	println("lookup", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	start = time.Now()
	_, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
	println("json marshal using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	start = time.Now()
	var buffer bytes.Buffer
	err = gob.NewEncoder(&buffer).Encode(r)
	if err != nil {
		t.Fatal(err)
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}
	println("gob marshal using:", time.Since(start).Nanoseconds()/1000000000, " sec")
	r.Close()

	r = New(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", COUNT/10, 10, "2013/1")
	if l.Len() != COUNT/10 {
		t.Errorf("should got %d, but we got %d", COUNT/10, l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			println(v.Value.(string))
		}
	}

	println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
}

func TestLookup(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	if v := r.Lookup("tester"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tester")
		} else {
			if s != "tester" {
				t.Errorf("expecting %s found %s", "tester", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("slow"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "slow")
		} else {
			if s != "slow" {
				t.Errorf("expecting %s found %s", "slow", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("water"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "water")
		} else {
			if s != "water" {
				t.Errorf("expecting %s found %s", "water", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("waterloo"); v != nil {
		t.Errorf("expecting nil found %v", v)
	}
	if v := r.Lookup("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "team")
		} else {
			if s != "team" {
				t.Errorf("expecting %s found %s", "team", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}

	if v := r.Lookup("te"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "te")
		} else {
			if s != "te" {
				t.Errorf("expecting %s found %s", "te", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "te")
	}
}

func TestLookupOnDisk(t *testing.T) {
	r := New(".")

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	r.Close()

	r = New(".")
	defer r.Destory()

	if v := r.Lookup("tester"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tester")
		} else {
			if s != "tester" {
				t.Errorf("expecting %s found %s", "tester", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("slow"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "slow")
		} else {
			if s != "slow" {
				t.Errorf("expecting %s found %s", "slow", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("water"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "water")
		} else {
			if s != "water" {
				t.Errorf("expecting %s found %s", "water", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if v := r.Lookup("waterloo"); v != nil {
		t.Errorf("expecting nil found %v", v)
	}
	if v := r.Lookup("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "team")
		} else {
			if s != "team" {
				t.Errorf("expecting %s found %s", "team", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}

	if v := r.Lookup("te"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "te")
		} else {
			if s != "te" {
				t.Errorf("expecting %s found %s", "te", s)
			}
		}
	} else {
		t.Errorf("expecting %s found nil", "te")
	}
}

func TestDelete(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	if v := r.Delete("tester"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tester")
		} else {
			if s != "tester" {
				t.Errorf("expecting %s found %s", "tester", s)
			}
		}
	}

	if v := r.Delete("slow"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "slow")
		} else {
			if s != "slow" {
				t.Errorf("expecting %s found %s", "slow", s)
			}
		}
	}

	if v := r.Delete("water"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "water")
		} else {
			if s != "water" {
				t.Errorf("expecting %s found %s", "water", s)
			}
		}
	}

	if v := r.Delete("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "team")
		} else {
			if s != "team" {
				t.Errorf("expecting %s found %s", "team", s)
			}
		}
	}
	if v := r.Lookup("water"); v != nil {
		t.Errorf("expecting nil found %v", v)
	}

	r.Insert("team", "tortugas")
	if v := r.Lookup("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tortugas")
		} else {
			if s != "tortugas" {
				t.Errorf("expecting %s found %s", "tortugas", s)
			}
		}
	}
}

func TestDeleteDisk(t *testing.T) {
	r := New(".")

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	if v := r.Lookup("tester"); v == nil {
		t.Error("expecting non nil")
	}

	println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	if v := r.Delete("tester"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tester")
		} else {
			if s != "tester" {
				t.Errorf("expecting %s found %s", "tester", s)
			}
		}
	}

	r.Close()

	r = New(".")

	if v := r.Lookup("tester"); v != nil {
		t.Error("expecting nil")
	}

	println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	if v := r.Delete("slow"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "slow")
		} else {
			if s != "slow" {
				t.Errorf("expecting %s found %s", "slow", s)
			}
		}
	}

	r.Close()

	r = New(".")

	if v := r.Lookup("slower"); v == nil {
		t.Error("expecting non nil")
	}

	if v := r.Delete("water"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "water")
		} else {
			if s != "water" {
				t.Errorf("expecting %s found %s", "water", s)
			}
		}
	}

	r.Close()
	r = New(".")
	defer r.Destory()

	if v := r.Lookup("water"); v != nil {
		t.Error("expecting nil")
	}

	if v := r.Delete("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "team")
		} else {
			if s != "team" {
				t.Errorf("expecting %s found %s", "team", s)
			}
		}
	}
	if v := r.Lookup("water"); v != nil {
		t.Errorf("expecting nil found %v", v)
	}

	r.Insert("team", "tortugas")
	if v := r.Lookup("team"); v != nil {
		if s, ok := v.(string); !ok {
			t.Errorf("expecting %s found nil", "tortugas")
		} else {
			if s != "tortugas" {
				t.Errorf("expecting %s found %s", "tortugas", s)
			}
		}
	}
}

func TestPrefix(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("timor", "timor")

	l := r.Prefix("t")
	if l.Len() != 5 {
		t.Errorf("l.Len() = %d expecting 5", l.Len())
	}
	for e := l.Front(); e != nil; e = e.Next() {
		switch v := e.Value.(string); v {
		case "test":
		case "tester":
		case "team":
		case "toast":
		case "timor":
		default:
			t.Errorf("unexpected element in list %s", v)
		}
	}
	l = r.Prefix("w")
	if l.Len() != 1 {
		t.Errorf("l.Len() = %d expecting 1", l.Len())
	}
	if v := l.Front().Value.(string); v != "water" {
		t.Errorf("unexpected element in list %s", v)
	}
	l = r.Prefix("slower")
	if l.Len() != 1 {
		t.Errorf("l.Len() = %d expecting 1", l.Len())
	}
	if v := l.Front().Value.(string); v != "slower" {
		t.Errorf("unexpected element in list %s", v)
	}
}
