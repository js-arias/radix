package radix

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

const COUNT = 500000

func TestCommon(t *testing.T) {

	stri := []byte("几个大盘那/个好")
	stri2 := []byte("几个大盘那/个好代码规范咖啡店老")
	stri4 := []byte("abcd")
	stri5 := []byte("abcd7")
	str6 := []byte("123/")
	str7 := []byte("123/456")
	str8 := []byte("abc哈124")
	str9 := []byte("aBc哈124而899")
	str10 := []byte("aBc哈124而89")
	str11 := []byte("aBc哈124*/&环境lk")
	str12 := []byte("aBc哈124*/&环境lk34lk")
	str13 := []byte("/#&*lk$@!plk0987738")
	str14 := []byte("/#&*lk$@!plk0987738344/098jk")
	str15 := []byte("fdja&&^%^002fdkajdk中就嗲司机93y388327")
	str16 := []byte("fdja&&^%^002fdkajdk中就嗲司机93bfdsau")
	str17 := []byte("$^89()dja&&^%^002fdkajdk中就嗲司机93好y388327")
	str18 := []byte("$^89()ja&&^%^002fdkajdk中就嗲司机93好fdsau")
	str19 := []byte("0299381000099988/HJDJDJJ&&&90()-=122:><发到你看范德萨接口接口大家看")
	str20 := []byte("0299381000099988/HJDJDJJ&&&90()-=122:><fdnfiahfihuiahif")
	str21 := []byte("搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就fdhdshfnhfkdjshkfh")
	str22 := []byte("搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就飞机哦司机你（98人")
	str23 := []byte("    ()(*&&^^^^%%$$##!!!~@#$^&*((?>><??搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就fdhdshfnhfkdjshkfh")
	str24 := []byte("    ()(*&&^^^^%%$$##!!!~@#$^&*((?>><??搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就飞机哦司机你（98人")

	comStr := common(stri, stri2)
	if string(comStr) != "几个大盘那/个好" {
		t.Error(comStr)
		t.Fail()
	}
	comStr1 := common(stri4, stri5)
	if string(comStr1) != "abcd" {
		t.Error(comStr1)
		t.Fail()
	}
	comStr2 := common(str6, str7)
	if string(comStr2) != "123/" {
		t.Error(comStr2)
		t.Fail()
	}
	comStr3 := common(str8, str9)
	if string(comStr3) != "a" {
		t.Error(comStr3)
		t.Fail()
	}

	comStr4 := common(str10, str9)
	if string(comStr4) != "aBc哈124而89" {
		t.Error(comStr4)
		t.Fail()
	}

	comStr5 := common(str11, str12)
	if string(comStr5) != "aBc哈124*/&环境lk" {
		t.Error(comStr5)
		t.Fail()
	}

	comStr6 := common(str13, str14)
	if string(comStr6) != "/#&*lk$@!plk0987738" {
		t.Error(comStr6)
		t.Fail()
	}
	comStr7 := common(str15, str16)
	if string(comStr7) != "fdja&&^%^002fdkajdk中就嗲司机93" {
		t.Error(comStr7)
		t.Fail()
	}
	comStr8 := common(str17, str18)
	if string(comStr8) != "$^89()" {
		t.Error(comStr8)
		t.Fail()
	}

	comStr9 := common(str19, str20)
	if string(comStr9) != "0299381000099988/HJDJDJJ&&&90()-=122:><" {
		t.Error(comStr9)
		t.Fail()
	}

	comStr10 := common(str21, str22)
	if string(comStr10) != "搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就" {
		t.Error(comStr10)
		t.Fail()
	}

	comStr11 := common(str23, str24)
	if string(comStr11) != "    ()(*&&^^^^%%$$##!!!~@#$^&*((?>><??搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就" {
		t.Error(comStr11)
		t.Fail()
	}

}

func TestBoundary(t *testing.T) {

	str1 := []byte{0xF9, 0x89, 0x93, 0xA2}
	str2 := []byte{0xF9, 0x89, 0x93, 0xA2}

	str3 := []byte{0xF1, 0x89, 0x93, 0xA2}
	str4 := []byte{0xF1, 0x89, 0x93, 0xA2}

	comStr := common([]byte("fndsbngjbask"+string(str1)), []byte("fndsbngjbask"+string(str2)))
	if string(comStr) != "fndsbngjbask" {
		t.Error(comStr)
		t.Fail()
	}
	comStr1 := common(str3, str4)
	com1 := []byte{0xF1, 0x89, 0x93, 0xA2}
	rs := rune(com1[0]&mask4)<<18 | rune(com1[1]&maskx)<<12 | rune(com1[2]&maskx)<<6 | rune(com1[3]&maskx)
	if rs <= rune3Max || MaxRune < rs {
		t.Error(rs <= rune3Max)
		t.Error(MaxRune < rs)
		t.Error(MaxRune)
		t.Error(rs)
	}
	if string(comStr1) != string(rs) {
		//t.Error(string(rs))
		t.Error(string(comStr1))
		t.Error(string(rs))
		t.Fail()
	}
}

func TestDeleteAll(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("te", "te")
	r.Insert("tester", "tester")

	if string(r.Delete("te")) != "te" {
		t.Error("delete not match")
	}
	if string(r.Delete("tester")) != "tester" {
		t.Error("delete not match")
	}
	if string(r.Delete("test")) != "test" {
		t.Error("delete not match")
	}

	if string(r.Delete("slow")) != "slow" {
		t.Error("delete not match")
	}

	if string(r.Delete("water")) != "water" {
		t.Error("delete not match")
	}

	for _, d := range r.Root.Children {
		t.Fatal("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestStorageGetSet(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	if err := r.StoragePut([]byte("key"), []byte("value")); err == nil {
		t.Error("should not be nil")
	}

	if _, err := r.StorageGet([]byte("key")); err == nil {
		t.Error("should not be nil")
	}

	//test empty get/put
	if err := r.StoragePut(nil, []byte("value")); err == nil {
		t.Error("should not be nil")
	}

	if _, err := r.StorageGet(nil); err == nil {
		t.Error("should not be nil")
	}

	if err := r.StoragePut([]byte("*key"), []byte("value")); err != nil {
		t.Error("should be nil")
	}

	if v, err := r.StorageGet([]byte("*key")); err != nil || string(v) != "value" {
		t.Errorf("expect value but got %s", string(v))
	}

}

func TestInsertion(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	for _, d := range r.Root.Children {
		if s := string(d.Value); decodeValueToKey(s) != string(d.Prefix) {
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

func TestGetChildrenCnt(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("1", "1")
	r.Insert("11", "11")
	r.Insert("12", "12")
	cnt := 0
	getInMemChildrenCount(r.Root.Children[0], &cnt)
	if cnt != 3 {
		t.Errorf("should be 3, but we got %d", cnt)
	}

	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[0], &cnt)
	if cnt != 1 {
		t.Errorf("should be 1, but we got %d", cnt)
	}

	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[1], &cnt)
	if cnt != 1 {
		t.Errorf("should be 1, but we got %d", cnt)
	}

	r.Insert("111", "111")
	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[0], &cnt)
	if cnt != 2 {
		t.Errorf("should be 2, but we got %d", cnt)
	}

	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[1], &cnt)
	if cnt != 1 {
		t.Errorf("should be 1, but we got %d", cnt)
	}

	r.Insert("1111", "1111")
	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[0], &cnt)
	if cnt != 3 {
		t.Errorf("should be 3, but we got %d", cnt)
	}

	cnt = 0
	getInMemChildrenCount(r.Root.Children[0].Children[1], &cnt)
	if cnt != 1 {
		t.Errorf("should be 1, but we got %d", cnt)
	}

	cnt = 0
	getInMemChildrenCount(r.Root.Children[0], &cnt)
	if cnt != 5 {
		t.Errorf("should be 5, but we got %d", cnt)
	}
}

func TestCas(t *testing.T) {
	r := Open(".")
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
	r := Open(".")
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

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestDeleteCombine(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("1", "1")
	r.Insert("11", "11")
	r.Insert("12", "12")
	r.Delete("1")
	r.Delete("11")

	r.Insert("2", "2")
	r.Insert("21", "21")
	r.Insert("22", "22")

	r.Delete("2")
	r.Delete("21")

	r.Delete("12")
	r.Delete("22")

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestDeleteLastNodeCombine(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("1", "1")
	r.Insert("11", "11")
	r.Insert("111", "111")
	r.Insert("12", "12")
	if string(r.Delete("1")) != "1" {
		log.Fatal("not match")
	}
	if string(r.Delete("12")) != "12" {
		log.Fatal("not match")
	}

	r.Insert("2", "2")
	r.Insert("21", "21")
	r.Insert("22", "22")
	r.Insert("211", "211")
	r.Insert("212", "212")

	if string(r.Delete("212")) != "212" {
		log.Fatal("not match")
	}

	if string(r.Delete("2")) != "2" {
		log.Fatal("not match")
	}
	if string(r.Delete("22")) != "22" {
		log.Fatal("not match")
	}

	if r.Delete("22") != nil {
		log.Fatal("not match")
	}

	if string(r.Delete("11")) != "11" {
		log.Fatal("not match")
	}

	if string(r.Delete("21")) != "21" {
		log.Fatal("not match")
	}

	if string(r.Delete("111")) != "111" {
		log.Fatal("not match")
	}

	if string(r.Delete("211")) != "211" {
		log.Fatal("not match")
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestDeleteLastNodeCombineOnDisk(t *testing.T) {
	r := Open(".")

	r.Insert("1", "1")
	r.Insert("11", "11")
	r.Insert("111", "111")
	r.Insert("12", "12")
	r.Close()

	r = Open(".")
	if string(r.Delete("1")) != "1" {
		log.Fatal("not match")
	}
	if string(r.Delete("12")) != "12" {
		log.Fatal("not match")
	}

	r.Insert("2", "2")
	r.Insert("21", "21")
	r.Insert("22", "22")
	r.Insert("211", "211")
	r.Insert("212", "212")
	r.Close()
	r = Open(".")
	defer r.Destory()

	if string(r.Delete("2")) != "2" {
		log.Fatal("not match")
	}
	if string(r.Delete("22")) != "22" {
		log.Fatal("not match")
	}

	if string(r.Delete("11")) != "11" {
		log.Fatal("not match")
	}

	if string(r.Delete("21")) != "21" {
		log.Fatal("not match")
	}

	if string(r.Delete("111")) != "111" {
		log.Fatal("not match")
	}

	if string(r.Delete("211")) != "211" {
		log.Fatal("not match")
	}

	if string(r.Delete("212")) != "212" {
		log.Fatal("not match")
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestRecursiveDeleteMany(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	count := 200

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)

		if string(old) != str {
			t.Errorf("delete value not match old %s expect %s", string(old), str)
			log.Println(r.Stats())
			t.Fatal()
		}
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestRecursiveDelete1(t *testing.T) {
	r := Open(".")
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

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	if s := r.Lookup("t"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Lookup("te"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Lookup("tes"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Lookup("test"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Lookup("teste"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Lookup("tester"); s != nil {
		t.Error("expecting nil")
	}
}

func TestDeleteDisk(t *testing.T) {
	r := Open(".")

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	if s := r.Lookup("tester"); s == nil {
		t.Error("expecting non nil")
	}

	if s := r.Delete("tester"); s != nil {
		if string(s) != "tester" {
			t.Errorf("expecting %s found %s", "tester", s)
		}
	}

	log.Println(r.Stats())

	r.Close()

	r = Open(".")

	if s := r.Lookup("tester"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Delete("slow"); s != nil {
		if string(s) != "slow" {
			t.Errorf("expecting %s found %s", "slow", s)
		}
	}

	r.Close()

	r = Open(".")

	if s := r.Lookup("slow"); s != nil {
		t.Error("expecting non nil")
	}

	if s := r.Delete("water"); s != nil {
		if string(s) != "water" {
			t.Errorf("expecting %s found %s", "water", s)
		}
	}

	r.Close()

	r = Open(".")
	defer r.Destory()

	if s := r.Lookup("water"); s != nil {
		t.Error("expecting nil")
	}

	if s := r.Delete("team"); s != nil {
		if string(s) != "team" {
			t.Errorf("expecting %s found %s", "team", s)
		}
	}
	if s := r.Lookup("team"); s != nil {
		t.Errorf("expecting nil found %v", s)
	}

	r.Insert("team", "tortugas")
	if s := r.Lookup("team"); s != nil {
		if string(s) != "tortugas" {
			t.Errorf("expecting %s found %s", "tortugas", s)
		}
	}
}

func TestLookupByPrefixAndDelimiter(t *testing.T) {
	r := Open(".")
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
		t.Errorf("should got 6, but we got %d", l.Len())
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
		}
	}

	l = r.LookupByPrefixAndDelimiter("test123", "/", 100, 100, "")
	if l.Len() != 1 {
		t.Errorf("should got 6, but we got %d", l.Len())
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
		}
	}
}

func TestLookupByPrefixAndDelimiterWith1Child(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("t", "test")
	r.Insert("te", "slow")
	r.Insert("tes", "water")
	r.Insert("test", "test")
	r.Insert("teste", "test")
	r.Insert("tester", "test")

	if r.GetFirstLevelChildrenCount("t") != 1 {
		t.Error("should be 1")
	}

	if r.GetFirstLevelChildrenCount("te") != 1 {
		t.Error("should be 1")
	}

	if r.GetFirstLevelChildrenCount("tes") != 1 {
		t.Error("should be 1")
	}

	if r.GetFirstLevelChildrenCount("test") != 1 {
		t.Error("should be 1")
	}

	if r.GetFirstLevelChildrenCount("teste") != 1 {
		t.Error("should be 1")
	}

	r.Delete("teste")
	r.Delete("test")
	r.Delete("tes")
	r.Delete("te")
	r.Delete("t")

	l := r.LookupByPrefixAndDelimiter("t", "/", 100, 100, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
		}
	}

	if internalKey := r.FindInternalKey("tester"); internalKey != "ktester" {
		t.Errorf("should be ktester but we got %s", internalKey)
	}

	if r.GetFirstLevelChildrenCount("t") != 0 {
		t.Error("should be 0")
	}
	if r.GetFirstLevelChildrenCount("tester") != 0 {
		t.Error("should be 0")
	}

	r.Delete("tester")

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}
}

func TestLookupByPrefixAndDelimiter_complex(t *testing.T) {
	r := Open(".")
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
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 10, but we got %d", l.Len())
	}
}

func TestLookupByPrefixAndDelimiter_emptyTree(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	l := r.LookupByPrefixAndDelimiter("", "#", 100, 100, "")
	if l.Len() != 0 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 0, but we got %d", l.Len())
	}
}

func TestLookupByPrefixAndDelimiter_emptyTreeAndArgs(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	l := r.LookupByPrefixAndDelimiter("", "", 100, 100, "")
	if l.Len() != 0 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 0, but we got %d", l.Len())
	}

	l = r.LookupByPrefixAndDelimiter("", "", 0, 0, "")
	if l.Len() != 0 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 0, but we got %d", l.Len())
	}
}

func TestLookupByPrefixAndDelimiter_delimiterNotExist(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("te#st", "te#st")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")
	r.Insert("test123/1//a", "test123/1//a")
	r.Insert("test123/2", "test123/2")
	r.Insert("test123//2", "test123//2")

	l := r.LookupByPrefixAndDelimiter("", "*", 100, 100, "")
	if l.Len() != 11 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 11, but we got %d", l.Len())
	}

	l = r.LookupByPrefixAndDelimiter("te", "*", 100, 100, "")
	if l.Len() != 7 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Printf("%+v", v.Value)
		}
		t.Errorf("should got 7, but we got %d", l.Len())
	}

	l = r.LookupByPrefixAndDelimiter("tes", "*", 100, 100, "")
	if l.Len() != 4 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 4, but we got %d", l.Len())
	}

	l = r.LookupByPrefixAndDelimiter("tes", "/", 100, 100, "")
	if l.Len() != 2 {
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
		t.Errorf("should got 2, but we got %d", l.Len())
	}
}

func TestLookupByPrefixAndDelimiter_limit(t *testing.T) {
	r := Open(".")
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
		t.Errorf("should got 3, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
	}

	l = r.LookupByPrefixAndDelimiter("t", "/", 10, 100, "")
	if l.Len() != 6 {
		t.Errorf("should got 6, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
	}
}

func TestLookupByPrefixAndDelimiter_limit_marker(t *testing.T) {
	r := Open(".")
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

	l := r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "test")
	if l.Len() != 2 {
		t.Errorf("should got 2, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Printf("%+v", v.Value)
		}
	}
}

func TestLookupByPrefixAndDelimiter_limit_marker1(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("test", "1")
	r.Insert("slow", "2")
	r.Insert("water", "3")
	r.Insert("slower", "4")
	r.Insert("tester", "5")
	r.Insert("team", "6")
	r.Insert("toast", "7")
	r.Insert("te", "te")
	r.Insert("test123/1", "8")
	r.Insert("test123/2", "9")
	r.Insert("test123//2", "10")

	l := r.LookupByPrefixAndDelimiter("t", "/", 5, 100, "te")
	if l.Len() != 4 {
		t.Errorf("should got 4, but we got %d", l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Printf("%+v", v.Value)
		}
	}

	for v := l.Front(); v != nil; v = v.Next() {
		log.Printf("%+v", v.Value.(*Tuple))
	}
}

func TestLookupByPrefixAndDelimiter_complex_many(t *testing.T) {
	r := Open(".")

	count := COUNT / 100

	start := time.Now()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("2013/%d", i)
		r.Insert(key, "")
		if i%10000 == 0 {
			print(".")
		}
	}
	log.Println("Insert", count, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")
	r.Close()

	r = Open(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	log.Println("lookup", count, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r.Close()

	r = Open(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", int32(count/10), 10, "2013/1")
	if l.Len() != count/10 {
		t.Errorf("should got %d, but we got %d", count/10, l.Len())
		for s := l.Front(); s != nil; s = s.Next() {
			log.Println(s.Value)
		}
	}

	log.Println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
}

func TestLookupByPrefixAndDelimiter_complex_many_bigkey(t *testing.T) {
	r := Open(".")

	start := time.Now()
	b := bytes.Buffer{}
	for i := 0; i < 1000; i++ {
		b.WriteByte('c')
	}

	count := 5000

	buf := b.String()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("2013/%d", i)
		r.Insert(key+buf, string(b.Bytes()))
		if i%10000 == 0 {
			print(".")
		}
	}

	r.Close()

	log.Println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$big key Insert", count, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r = Open(".")

	start = time.Now()
	l := r.LookupByPrefixAndDelimiter("2", "/", 100, 10, "")
	if l.Len() != 1 {
		t.Errorf("should got 1, but we got %d", l.Len())
	}
	log.Println("lookup", count, "using:", time.Since(start).Nanoseconds()/1000000000, " sec")

	r.Close()

	r = Open(".")
	defer r.Destory()

	start = time.Now()
	l = r.LookupByPrefixAndDelimiter("2", "#", int32(count/10), 10, "2013/1")
	if l.Len() != count/10 {
		t.Errorf("should got %d, but we got %d", count/10, l.Len())
		for v := l.Front(); v != nil; v = v.Next() {
			log.Println(v.Value)
		}
	}

	log.Println("bad lookup:", time.Since(start).Nanoseconds()/1000000000, " sec")
}

func TestLookup(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

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
	r := Open(".")

	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	r.Insert("slower", "slower")
	r.Insert("tester", "tester")
	r.Insert("team", "team")
	r.Insert("toast", "toast")
	r.Insert("te", "te")

	r.Close()

	r = Open(".")
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
	r := Open(".")
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
	r := Open(".")
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

	l = r.Prefix("x")
	if l.Len() != 0 {
		t.Error("should be zero")
	}
}

func TestDumpEmptyTree(t *testing.T) {
	r := Open(".")
	defer r.Destory()
	if r.DumpMemTree() != nil {
		t.Error("should be nil")
	}

	if r.DumpTree() != nil {
		t.Error("should be nil")
	}
}

func TestConcurrent(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	count := 2000

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	goroutineCount := 4

	wg := sync.WaitGroup{}
	wg.Add(goroutineCount)
	f := func() {
		for i := 0; i < count; i++ {
			str := fmt.Sprintf("%d", i)
			buf, version := r.GetWithVersion(str)
			if version != 0 || string(buf) != str {
				t.FailNow()
			}
		}
		log.Println("done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go f()
	}

	wg.Wait()

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)

		if string(old) != str {
			t.Errorf("delete value not match old %s expect %s", string(old), str)
		}
	}

	log.Printf("%+v", r.Root)

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestOnDiskDeleteCut(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.SetMaxInMemoryNodeCount(2)

	count := 21

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)

		if string(old) != str {
			t.Errorf("delete value not match old %s expect %s", string(old), str)
		}
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestOnDiskDelete(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.SetMaxInMemoryNodeCount(10)

	count := 2000

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)

		if string(old) != str {
			t.Errorf("delete value not match old %s expect %s", string(old), str)
		}
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestCutEdge(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	r.SetMaxInMemoryNodeCount(600)

	count := 1000

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)

		if string(old) != str {
			t.Errorf("delete value not match old %s expect %s", string(old), str)
		}
	}

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestConcurrentReadDelete(t *testing.T) {
	runtime.GOMAXPROCS(4)
	r := Open(".")
	defer r.Destory()

	count := COUNT / 100

	log.Println("total count", count)

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	goroutineCount := 5

	wg := sync.WaitGroup{}
	wg.Add(goroutineCount)
	f := func(start, end int) {
		log.Println("start-end", start, end)
		for i := start; i < end; i++ {
			str := fmt.Sprintf("%d", i)
			buf, version := r.GetWithVersion(str)
			if version != 0 || string(buf) != str {
				t.FailNow()
			}
		}
		log.Println("read done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go f(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	wg.Wait()

	wg.Add(goroutineCount)

	d := func(start, end int) {
		for i := start; i < end; i++ {
			str := fmt.Sprintf("%d", i)
			old := r.Delete(str)

			if string(old) != str {
				t.Errorf("delete value not match old %s expect %s", string(old), str)
			}
		}
		log.Println("delete done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go d(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	wg.Wait()

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestBackup(t *testing.T) {
	runtime.GOMAXPROCS(4)
	r := Open(".")
	defer r.Destory()

	count := 1000000

	log.Println("total count", count)

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		r.Insert(str, str)
	}

	goroutineCount := 5

	ch := r.Backup("bakdb")

	wg := sync.WaitGroup{}
	wg.Add(goroutineCount)
	f := func(start, end int) {
		log.Println("start-end", start, end)
		for i := start; i < end; i++ {
			str := fmt.Sprintf("%d", i)
			buf, version := r.GetWithVersion(str)
			if version != 0 || string(buf) != str {
				t.FailNow()
			}
		}
		log.Println("read done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go f(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	wg.Wait()

	wg.Add(goroutineCount)

	d := func(start, end int) {
		for i := start; i < end; i++ {
			str := fmt.Sprintf("%d", i)
			old := r.Delete(str)

			if string(old) != str {
				t.Errorf("delete value not match old %s expect %s", string(old), str)
			}
		}
		log.Println("delete done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go d(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	wg.Wait()

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}

	<-ch
}

func TestConcurrentRandomReadWrite(t *testing.T) {
	runtime.GOMAXPROCS(4)
	r := Open(".")
	defer r.Destory()

	log.Println(r.Stats())

	count := COUNT

	goroutineCount := 5

	wg := sync.WaitGroup{}
	wg.Add(2 * goroutineCount)
	f := func() {
		for i := 0; i < 100000; i++ {
			if i%1000 == 0 {
				print("r")
			}
			k := rand.Int31n(int32(count))
			str := fmt.Sprintf("%d", k)
			r.GetWithVersion(str)
		}
		log.Println("read done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go f()
	}

	w := func(start, end int) {
		for i := start; i < end; i++ {
			if i == start || i == end-1 {
				log.Println("insert....", i)
			}
			if i%1000 == 0 {
				print("w")
			}

			str := fmt.Sprintf("%d", i)
			if b, err := r.Insert(str, str); b != nil || err != nil {
				log.Fatal(b, err)
			}
		}
		wg.Done()
		log.Println("insert done", start, end)
	}

	for i := 0; i < goroutineCount; i++ {
		go w(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	wg.Wait()

	// log.Println(r.Stats())

	wg.Add(goroutineCount)

	d := func(start, end int) {
		for i := start; i < end; i++ {
			if i%1000 == 0 {
				print("d")
			}
			str := fmt.Sprintf("%d", i)
			old := r.Delete(str)

			if string(old) != str {
				t.Errorf("delete value not match old %s expect %s", string(old), str)
				log.Fatalf("delete value not match old %s expect %s", string(old), str)
			}
		}
		log.Println("delete done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go d(i*count/goroutineCount, (i+1)*count/goroutineCount)
	}

	log.Println("starting delete")

	wg.Wait()

	for _, d := range r.Root.Children {
		t.Errorf("should be empty tree %+v", d)
	}

	if !r.h.store.IsEmpty(r.snapshot) {
		t.Error("should be empty", r.Stats())
	}

	for i := 0; i < 2*count; i++ {
		str := fmt.Sprintf("%d", i)
		old := r.Delete(str)
		if old != nil {
			t.Error("expect nil")
		}
	}
}

func TestSimpleInsert(t *testing.T) {
	r := Open(".")
	defer r.Destory()

	runtime.GOMAXPROCS(4)

	count := 10000

	goroutineCount := 5

	wg := sync.WaitGroup{}
	wg.Add(goroutineCount)
	f := func() {
		for i := 0; i < 10000; i++ {
			if i%1000 == 0 {
				print("r")
			}
			k := rand.Int31n(int32(count))
			str := fmt.Sprintf("%d", k)
			r.GetWithVersion(str)
		}
		log.Println("read done")
		wg.Done()
	}

	for i := 0; i < goroutineCount; i++ {
		go f()
	}

	runtime.Gosched()

	r.Insert("200", "200")
	r.Insert("201", "201")
	r.Insert("0", "0")
	wg.Wait()
}
