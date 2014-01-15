package radix

const (
	ROOT_SEQ            = -1
	INTERNAL_KEY_PREFIX = "k"
)

// return the common string, require utf8 string
func common(s, o string) string {
	i := 0
	for ; i < len(s) && i < len(o); i++ {
		if s[i] != o[i] {
			break
		}
	}
	return s[:i]
}

func encodeValueToInternalKey(value string) string {
	return INTERNAL_KEY_PREFIX + value
}

func decodeValueToKey(value string) string {
	return value[1:]
}
