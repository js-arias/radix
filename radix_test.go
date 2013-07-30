// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package radix

import "testing"

func TestInsertion(t *testing.T) {
	r := New()
	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	for d := r.desc; d != nil; d = d.sis {
		if v := d.value.(string); v != d.prefix {
			t.Errorf("d.value = %s, want %s", v, d.prefix)
		}
	}
	r.Insert("slower", "slower")
	for d := r.desc; d != nil; d = d.sis {
		if d.prefix != "slow" {
			continue
		}
		if v := d.value.(string); v != "slow" {
			t.Errorf("d.value = %s, want %s", v, d.prefix)
		}
		if d.desc.prefix == "er" {
			if v := d.desc.value.(string); v != "slower" {
				t.Errorf("d.desc.value = %s, want %s", v, "slower")
			}
			break
		}
		t.Errorf("d.desc.prefix = %s, want %s", d.desc.prefix, "er")
	}
	r.Insert("team", "team")
	r.Insert("tester", "tester")
	var ok bool
	for d := r.desc; d != nil; d = d.sis {
		if d.prefix == "te" {
			if v, ok := d.value.(string); ok {
				t.Errorf("d.value = %s, want nil", v)
			}
			ok = true
			for n := d.desc; n != nil; n = n.sis {
				switch v := n.value.(string); n.prefix {
				case "am":
					if v != "team" {
						t.Errorf("n.value = %s, want %s", v, "team")
					}
				case "st":
					if v != "test" {
						t.Errorf("n.value = %s, want %s", v, "test")
					}
					if n.desc == nil {
						t.Errorf("nil value unexpected in n.desc")
					}
				default:
					t.Errorf("n.value = %s, want %s or %s", v, "team", "tester")
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
	for d := r.desc; d != nil; d = d.sis {
		if d.prefix == "te" {
			v := d.value.(string)
			if v != "te" {
				t.Errorf("d.value = %s, want %s", v, "te")
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

func TestLookup(t *testing.T) {
	r := New()
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
}

func TestDelete(t *testing.T) {
	r := New()
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

func TestPrefix(t *testing.T) {
	r := New()
	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("timor", "timor")

	l := r.Prefix("t")

	for e := l.Front(); e != nil; e = e.Next() {
		println("value:", e.Value.(string))
	}

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
