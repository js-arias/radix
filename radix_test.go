// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package radix

import (
	"bytes"
	"fmt"
	"log"
	"testing"
	"time"
)

const COUNT = 10000

var _ = bytes.HasPrefix
var _ = fmt.Scan
var _ = time.Now

func TestDeleteAll(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("te", "test")
	r.Insert("tester", "test")

	r.Delete("te")
	r.Delete("tester")
	r.Delete("test")
	r.Delete("slow")
	r.Delete("water")

	for _, d := range r.Root.Children {
		t.Fatal("should be empty tree %+v", d)
	}

	log.Println(r.Stats())
}

func TestInsertion(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	for _, d := range r.Root.Children {
		if s := d.Value; decodeValueToKey(s) != d.Prefix {
			t.Errorf("d.Value = %s, want %s", s, d.Prefix)
		}
	}
	r.Insert("slower", "slower")
	r.Insert("team", "team")
	r.Insert("tester", "tester")

	r.Insert("te", "te")

	if _, err := r.Insert("slow", "slow"); err == nil {
		t.Errorf("expecting error at insert")
	}
	if _, err := r.Insert("water", "water"); err == nil {
		t.Errorf("expecting error at insert")
	}
	if _, err := r.Insert("team", "team"); err == nil {
		t.Errorf("expecting error at insert")
	}
}

func TestCas(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("key", "value")
	{
		value, version := r.GetWithVersion("key")
		if string(value) != "value" {
			t.Error("value not match")
		}

		if version != 0 {
			t.Error("version not match")
		}
	}

	{
		// log.Println("test cas")
		value, err := r.CAS("key", "xx", 0)
		if err != nil {
			t.Error(err)
		}

		if value == nil || string(value) != "value" {
			t.Error("value not match", value)
		}

		value, version := r.GetWithVersion("key")
		if string(value) != "xx" {
			t.Error("value not match")
		}

		if version != 1 {
			t.Error("version not match")
		}
	}

	{
		// log.Println("test cas should return error")
		value, err := r.CAS("key", "xx", 0)
		if err == nil || value != nil {
			t.Error("should raise error")
		}
	}

	r.Insert("key1", "value1")
	for i := 0; i < 100; i++ {
		r.CAS("key1", "xx", int64(i))
		_, version := r.GetWithVersion("key1")
		if version != int64(i+1) {
			t.Errorf("version not match %d - %d", i+1, version)
		}
	}
}

func TestRecursiveDelete(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("t", "test")
	r.Insert("te", "slow")
	r.Insert("tes", "water")
	r.Insert("test", "test")
	r.Insert("teste", "test")
	r.Insert("tester", "test")

	r.Delete("tester")
	r.Delete("teste")
	r.Delete("test")
	r.Delete("tes")
	r.Delete("te")
	r.Delete("t")

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	log.Println(r.Stats())
}

func TestRecursiveDeleteMany(t *testing.T) {
	r := New(".")
	defer r.Destory()

	count := 1000

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Delete(str)
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	log.Println(r.Stats())
}

func TestRecursiveDelete1(t *testing.T) {
	r := New(".")
	defer r.Destory()

	r.Insert("t", "test")
	r.Insert("te", "slow")
	r.Insert("tes", "water")
	r.Insert("test", "test")
	r.Insert("teste", "test")
	r.Insert("tester", "test")

	r.Delete("teste")
	r.Delete("test")
	r.Delete("tes")
	r.Delete("te")
	r.Delete("t")
	r.Delete("tester")

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}
}

func TestDeleteDisk(t *testing.T) {
	r := New(".")

	log.Println("***************************************************************************")

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.DumpTree()
	r.Insert("water", "water")
	r.DumpTree()
	r.Insert("slower", "slower")
	r.DumpTree()
	r.Insert("tester", "tester")
	r.DumpTree()
	r.Insert("team", "team")
	r.DumpTree()
	r.Insert("toast", "toast")
	r.DumpTree()
	r.Insert("te", "te")

	r.DumpTree()

	if s := r.Lookup("tester"); s == nil {
		t.Error("expecting non nil")
	}

	r.DumpTree()

	if s := r.Delete("tester"); s != nil {
		if string(s) != "tester" {
			t.Errorf("expecting %s found %s", "tester", s)
		}
	}

	log.Println("after delete tester")

	r.DumpTree()

	r.Close()

	r = New(".")

	if s := r.Lookup("tester"); s != nil {
		t.Error("expecting nil")
	}

	r.DumpTree()

	if s := r.Delete("slow"); s != nil {
		if string(s) != "slow" {
			t.Errorf("expecting %s found %s", "slow", s)
		}
	}

	log.Println("after delete slow")

	r.DumpTree()

	r.Close()

	r = New(".")

	if s := r.Lookup("slower"); s == nil {
		t.Error("expecting non nil")
	}

	if s := r.Delete("water"); s != nil {
		if string(s) != "water" {
			t.Errorf("expecting %s found %s", "water", s)
		}
	}

	r.Close()
	r = New(".")

	log.Println("after delete water")

	r.DumpTree()
	r.Close()

	r = New(".")
	defer r.Destory()

	if s := r.Lookup("water"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Delete("team"); s != nil {
		if string(s) != "team" {
			t.Errorf("expecting %s found %s", "team", s)
		}
	}
	if s := r.Lookup("water"); s != nil {
		t.Errorf("expecting nil found %v", s)
	}

	r.Insert("team", "tortugas")
	if s := r.Lookup("team"); s != nil {
		if string(s) != "tortugas" {
			t.Errorf("expecting %s found %s", "tortugas", s)
		}
	}

	log.Println("***************************************************************************")
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
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
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
	if l.Len() != 10 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 10, but we got %d", l.Len())
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
			log.Println(v.Value)
		}
	}

	l = r.LookupByPrefixAndDelimiter("t", "/", 3, 100, "")
	if l.Len() != 3 {
		t.Errorf("should got 2, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
	}

	l = r.LookupByPrefixAndDelimiter("t", "/", 10, 100, "")
	if l.Len() != 6 {
		t.Errorf("should got 2, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
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

	log.Println("---------------------------")
	l := r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "test")
	if l.Len() != 3 {
		t.Errorf("should got 3, but we got %d", l.Len())
		log.Println("================================")
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		log.Println("---------------------------")
	}

	l = r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "te")
	if l.Len() != 5 {
		t.Errorf("should got 5, but we got %d", l.Len())
		log.Println("================================")
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		log.Println("---------------------------")
	}
}

func TestLookupByPrefixAndDelimiter_complex_many(t *testing.T) {
	r := New(".")

	start := time.Now()
	for i := 0; i < COUNT; i++ {
		key := fmt.Sprintf("2013/%d", i)
		r.Insert(key, "")
		if i%10000 == 0 {
			print(".")
		}
	}
	log.Println("Insert", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")
	r.Close()

	r = New(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	log.Println("lookup", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r.Close()

	r = New(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", COUNT/10, 10, "2013/1")
	if l.Len() != COUNT/10 {
		t.Errorf("should got %d, but we got %d", COUNT/10, l.Len())
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
		}
	}

	log.Println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
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
		r.Insert(key+buf, string(b.Bytes()))
		if i%10000 == 0 {
			print(".")
		}
	}

	r.Close()

	log.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$big key Insert", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r = New(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	log.Println("lookup", COUNT, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r.Close()

	r = New(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", COUNT/10, 10, "2013/1")
	if l.Len() != COUNT/10 {
		t.Errorf("should got %d, but we got %d", COUNT/10, l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
	}

	log.Println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
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
	log.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
	if s := r.Lookup("tester"); s != nil {
		if string(s) != "tester" {
			t.Errorf("expecting %s found %s", "tester", s)
		}

	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("slow"); s != nil {
		if string(s) != "slow" {
			t.Errorf("expecting %s found %s", "slow", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("water"); s != nil {
		if string(s) != "water" {
			t.Errorf("expecting %s found %s", "water", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("waterloo"); s != nil {
		t.Errorf("expecting nil found %v", s)
	}
	if s := r.Lookup("team"); s != nil {
		if string(s) != "team" {
			t.Errorf("expecting %s found %s", "team", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}

	if s := r.Lookup("te"); s != nil {
		if string(s) != "te" {
			t.Errorf("expecting %s found %s", "te", s)
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

	if s := r.Lookup("tester"); s != nil {
		if string(s) != "tester" {
			t.Errorf("expecting %s found %s", "tester", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("slow"); s != nil {
		if string(s) != "slow" {
			t.Errorf("expecting %s found %s", "slow", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("water"); s != nil {
		if string(s) != "water" {
			t.Errorf("expecting %s found %s", "water", s)
		}
	} else {
		t.Errorf("expecting %s found nil", "tester")
	}
	if s := r.Lookup("waterloo"); s != nil {
		t.Errorf("expecting nil found %v", s)
	}
	if s := r.Lookup("team"); s != nil {

		if string(s) != "team" {
			t.Errorf("expecting %s found %s", "team", s)
		}

	} else {
		t.Errorf("expecting %s found nil", "tester")
	}

	if s := r.Lookup("te"); s != nil {
		if string(s) != "te" {
			t.Errorf("expecting %s found %s", "te", s)
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

	if s := r.Delete("tester"); s != nil {
		if string(s) != "tester" {
			t.Errorf("expecting %s found %s", "tester", s)
		}
	}

	if s := r.Delete("slow"); s != nil {
		if string(s) != "slow" {
			t.Errorf("expecting %s found %s", "slow", s)
		}
	}

	if s := r.Delete("water"); s != nil {
		if string(s) != "water" {
			t.Errorf("expecting %s found %s", "water", s)
		}
	}

	if s := r.Delete("team"); s != nil {
		if string(s) != "team" {
			t.Errorf("expecting %s found %s", "team", s)
		}
	}
	if s := r.Lookup("water"); s != nil {
		t.Errorf("expecting nil found %v", s)
	}

	r.Insert("team", "tortugas")
	if s := r.Lookup("team"); s != nil {
		if string(s) != "tortugas" {
			t.Errorf("expecting %s found %s", "tortugas", s)
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
	if s := l.Front().Value.(string); s != "water" {
		t.Errorf("unexpected element in list %s", s)
	}
	l = r.Prefix("slower")
	if l.Len() != 1 {
		t.Errorf("l.Len() = %d expecting 1", l.Len())
	}
	if s := l.Front().Value.(string); s != "slower" {
		t.Errorf("unexpected element in list %s", s)
	}
}
