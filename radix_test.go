// Copyright (c) 2013, J. Salvador Arias <jsalarias@csnat.unt.edu.ar>
// All rights reserved.
// Distributed under BSD2 license that can be found in the LICENSE file.

package radix

import "testing"

// added from https://github.com/ngaut/radix
func TestCommon(t *testing.T) {
	stri := []rune("几个大盘那/个好")
	stri2 := []rune("几个大盘那/个好代码规范咖啡店老")
	stri4 := []rune("abcd")
	stri5 := []rune("abcd7")
	str6 := []rune("123/")
	str7 := []rune("123/456")
	str8 := []rune("abc哈124")
	str9 := []rune("aBc哈124而899")
	str10 := []rune("aBc哈124而89")
	str11 := []rune("aBc哈124*/&环境lk")
	str12 := []rune("aBc哈124*/&环境lk34lk")
	str13 := []rune("/#&*lk$@!plk0987738")
	str14 := []rune("/#&*lk$@!plk0987738344/098jk")
	str15 := []rune("fdja&&^%^002fdkajdk中就嗲司机93y388327")
	str16 := []rune("fdja&&^%^002fdkajdk中就嗲司机93bfdsau")
	str17 := []rune("$^89()dja&&^%^002fdkajdk中就嗲司机93好y388327")
	str18 := []rune("$^89()ja&&^%^002fdkajdk中就嗲司机93好fdsau")
	str19 := []rune("0299381000099988/HJDJDJJ&&&90()-=122:><发到你看范德萨接口接口大家看")
	str20 := []rune("0299381000099988/HJDJDJJ&&&90()-=122:><fdnfiahfihuiahif")
	str21 := []rune("搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就fdhdshfnhfkdjshkfh")
	str22 := []rune("搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就飞机哦司机你（98人")
	str23 := []rune("    ()(*&&^^^^%%$$##!!!~@#$^&*((?>><??搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就fdhdshfnhfkdjshkfh")
	str24 := []rune("    ()(*&&^^^^%%$$##!!!~@#$^&*((?>><??搭建积分地撒谎ifh**&&&………………————+：；》《MKKKKKK*分阶段看 发动机类似就飞机哦司机你（98人")

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

func TestInsertion(t *testing.T) {
	r := New()
	r.Insert("test", "test")
	r.Insert("slow", "slow")
	r.Insert("water", "water")
	for d := r.root.desc; d != nil; d = d.sis {
		if v := d.value.(string); v != string(d.prefix) {
			t.Errorf("d.value = %s, want %s", v, d.prefix)
		}
	}
	r.Insert("slower", "slower")
	for d := r.root.desc; d != nil; d = d.sis {
		if string(d.prefix) != "slow" {
			continue
		}
		if v := d.value.(string); v != "slow" {
			t.Errorf("d.value = %s, want %s", v, d.prefix)
		}
		if string(d.desc.prefix) == "er" {
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
	for d := r.root.desc; d != nil; d = d.sis {
		if string(d.prefix) == "te" {
			if v, ok := d.value.(string); ok {
				t.Errorf("d.value = %s, want nil", v)
			}
			ok = true
			for n := d.desc; n != nil; n = n.sis {
				switch v := n.value.(string); string(n.prefix) {
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
	for d := r.root.desc; d != nil; d = d.sis {
		if string(d.prefix) == "te" {
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

func TestIterator(t *testing.T) {
	sl := []string{
		"test",
		"slow",
		"water",
		"slower",
		"tester",
		"team",
		"toast",
		"timor",
		"te",
		"a",
		"aa",
		"aaaaaaa",
		"aaaa",
	}
	r := New()
	for _, s := range sl {
		r.Insert(s, s)
	}
	it := r.Iterator()
	if it == nil {
		t.Errorf("nil iterator")
	}
	i := 0
	prev := it.Value.(string)
	for ; it != nil; it = it.Next() {
		if it.Value.(string) < prev {
			t.Errorf("iterator withour alphabetical order")
		}
		i++
	}
	if i != len(sl) {
		t.Errorf("iterator fail to navigate the radix, expecting %d found %d", len(sl), i)
	}
}
