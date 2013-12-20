package radix

const (
	ROOT_SEQ            = -1
	INTERNAL_KEY_PREFIX = "k"
)

// return the common string
func common(s, o string) string {
	max, min := s, o
	if len(max) < len(min) {
		max, min = min, max
	}
	var str []rune
	for i, r := range min {
		if r != rune(max[i]) {
			break
		}
		if str == nil {
			str = []rune{r}
		} else {
			str = append(str, r)
		}
	}
	return string(str)
}

func encodeValueToInternalKey(value string) string {
	return INTERNAL_KEY_PREFIX + value
}

func decodeValueToKey(value string) string {
	return value[1:]
}
