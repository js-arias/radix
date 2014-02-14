package radix

const (
	ROOT_SEQ            = -1
	INTERNAL_KEY_PREFIX = 'k'
)

const (
	RuneError = '\uFFFD'     // the "error" Rune or "Unicode replacement character"
	RuneSelf  = 0x80         // characters below Runeself are represented as themselves in a single byte.
	MaxRune   = '\U0010FFFF' // Maximum valid Unicode code point.
	UTFMax    = 4            // maximum number of bytes of a UTF-8 encoded Unicode character.
)

const (
	surrogateMin = 0xD800
	surrogateMax = 0xDFFF
)
const (
	t1 = 0x00 // 0000 0000
	tx = 0x80 // 1000 0000
	t2 = 0xC0 // 1100 0000
	t3 = 0xE0 // 1110 0000
	t4 = 0xF0 // 1111 0000
	t5 = 0xF8 // 1111 1000

	maskx = 0x3F // 0011 1111
	mask2 = 0x1F // 0001 1111
	mask3 = 0x0F // 0000 1111
	mask4 = 0x07 // 0000 0111

	rune1Max = 1<<7 - 1
	rune2Max = 1<<11 - 1
	rune3Max = 1<<16 - 1
)

// return the common string, require utf8 string
// func common(s []byte, t []byte) []byte {
// 	var rs, rt rune
// 	ind := 0
// 	if len(s) > 0 && len(t) > 0 {
// 		for len(s)-ind > 0 && len(t)-ind > 0 {
// 			if s[ind] < tx && t[ind] < tx && s[ind] == t[ind] {
// 				ind++
// 				continue
// 			}
// 			// unexpected continuation byte?
// 			if s[ind] < t2 || t[ind] < t2 {
// 				break
// 			}
// 			if len(s)-ind < 2 || len(t)-ind < 2 {
// 				break
// 			}

// 			if s[ind+1] < tx || t2 <= s[ind+1] || t[ind+1] < tx || t2 <= t[ind+1] {
// 				break
// 			}

// 			// 2-byte, 11-bit sequence?
// 			if s[ind] < t3 && t[ind] < t3 {
// 				rs = rune(s[ind]&mask2)<<6 | rune(s[ind+1]&maskx)
// 				rt = rune(t[ind]&mask2)<<6 | rune(t[ind+1]&maskx)
// 				if rt <= rune1Max || rs <= rune1Max {
// 					break
// 				}
// 				if rs == rt {
// 					ind += 2
// 					continue
// 				}
// 			}

// 			// need second continuation byte
// 			if len(s)-ind < 3 || len(t)-ind < 3 {
// 				break
// 			}

// 			if s[ind+2] < tx || t2 <= s[ind+2] || t[ind+2] < tx || t2 <= t[ind+2] {
// 				break
// 			}

// 			// 3-byte, 16-bit sequence?
// 			if s[ind] < t4 && t[ind] < t4 {
// 				rs = rune(s[ind]&mask3)<<12 | rune(s[ind+1]&maskx)<<6 | rune(s[ind+2]&maskx)
// 				rt = rune(t[ind]&mask3)<<12 | rune(t[ind+1]&maskx)<<6 | rune(t[ind+2]&maskx)
// 				if rs <= rune2Max || rs <= rune2Max {
// 					break
// 				}
// 				if surrogateMin <= rs && rs <= surrogateMax || surrogateMin <= rt && rt <= surrogateMax {
// 					break
// 				}
// 				if rs == rt {
// 					ind += 3
// 					continue
// 				}
// 			}

// 			// need third continuation byte
// 			if len(s)-ind < 4 || len(t)-ind < 4 {
// 				break
// 			}

// 			if s[ind+3] < tx || t2 <= s[ind+3] || t[ind+3] < tx || t2 <= t[ind+3] {
// 				break
// 			}

// 			// 4-byte, 21-bit sequence?
// 			if s[ind] < t5 && t[ind] < t5 {
// 				rs = rune(s[ind]&mask4)<<18 | rune(s[ind+1]&maskx)<<12 | rune(s[ind+2]&maskx)<<6 | rune(s[ind+3]&maskx)
// 				rt = rune(t[ind]&mask4)<<18 | rune(t[ind+1]&maskx)<<12 | rune(t[ind+2]&maskx)<<6 | rune(t[ind+3]&maskx)
// 				if rs <= rune3Max || MaxRune < rs || rt <= rune3Max || MaxRune < rt {
// 					break
// 				}
// 				if rs == rt {
// 					ind += 4
// 					continue
// 				}

// 			} else {
// 				break
// 			}
// 		}

// 	}

// 	if ind == 0 {
// 		return nil
// 	}

// 	return cloneByteSlice(s[:ind])
// }

//seems return string is a little bit faster
func common(s []byte, t []byte) string {
	var rs, rt rune
	ind := 0
	if len(s) > 0 && len(t) > 0 {
		for len(s)-ind > 0 && len(t)-ind > 0 {
			if s[ind] < tx && t[ind] < tx && s[ind] == t[ind] {
				ind++
				continue
			}
			// unexpected continuation byte?
			if s[ind] < t2 || t[ind] < t2 {
				break
			}
			if len(s)-ind < 2 || len(t)-ind < 2 {
				break
			}

			if s[ind+1] < tx || t2 <= s[ind+1] || t[ind+1] < tx || t2 <= t[ind+1] {
				break
			}

			// 2-byte, 11-bit sequence?
			if s[ind] < t3 && t[ind] < t3 {
				rs = rune(s[ind]&mask2)<<6 | rune(s[ind+1]&maskx)
				rt = rune(t[ind]&mask2)<<6 | rune(t[ind+1]&maskx)
				if rt <= rune1Max || rs <= rune1Max {
					break
				}
				if rs == rt {
					ind += 2
					continue
				}
			}

			// need second continuation byte
			if len(s)-ind < 3 || len(t)-ind < 3 {
				break
			}

			if s[ind+2] < tx || t2 <= s[ind+2] || t[ind+2] < tx || t2 <= t[ind+2] {
				break
			}

			// 3-byte, 16-bit sequence?
			if s[ind] < t4 && t[ind] < t4 {
				rs = rune(s[ind]&mask3)<<12 | rune(s[ind+1]&maskx)<<6 | rune(s[ind+2]&maskx)
				rt = rune(t[ind]&mask3)<<12 | rune(t[ind+1]&maskx)<<6 | rune(t[ind+2]&maskx)
				if rs <= rune2Max || rs <= rune2Max {
					break
				}
				if surrogateMin <= rs && rs <= surrogateMax || surrogateMin <= rt && rt <= surrogateMax {
					break
				}
				if rs == rt {
					ind += 3
					continue
				}
			}

			// need third continuation byte
			if len(s)-ind < 4 || len(t)-ind < 4 {
				break
			}

			if s[ind+3] < tx || t2 <= s[ind+3] || t[ind+3] < tx || t2 <= t[ind+3] {
				break
			}

			// 4-byte, 21-bit sequence?
			if s[ind] < t5 && t[ind] < t5 {
				rs = rune(s[ind]&mask4)<<18 | rune(s[ind+1]&maskx)<<12 | rune(s[ind+2]&maskx)<<6 | rune(s[ind+3]&maskx)
				rt = rune(t[ind]&mask4)<<18 | rune(t[ind+1]&maskx)<<12 | rune(t[ind+2]&maskx)<<6 | rune(t[ind+3]&maskx)
				if rs <= rune3Max || MaxRune < rs || rt <= rune3Max || MaxRune < rt {
					break
				}
				if rs == rt {
					ind += 4
					continue
				}

			} else {
				break
			}
		}

	}

	if ind == 0 {
		return ""
	}

	return string(s[:ind])
}

func encodeValueToInternalKey(value []byte) []byte {
	s := make([]byte, len(value)+1)
	copy(s[1:], value)
	s[0] = INTERNAL_KEY_PREFIX
	return s
}

func decodeValueToKey(value string) string {
	return value[1:]
}

func cloneByteSlice(b []byte) []byte {
	s := make([]byte, len(b))
	copy(s, b)
	return s
}
