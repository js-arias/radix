package radix

import (
	"errors"
	"fmt"
	"github.com/ngaut/megajson/scanner"
	"io"
	"strconv"
)

type radDiskNodeJSONDecoder struct {
	s scanner.Scanner
}

func NewradDiskNodeJSONDecoder(r io.Reader) *radDiskNodeJSONDecoder {
	return &radDiskNodeJSONDecoder{s: scanner.NewScanner(r)}
}

func NewradDiskNodeJSONScanDecoder(s scanner.Scanner) *radDiskNodeJSONDecoder {
	return &radDiskNodeJSONDecoder{s: s}
}

type int64JSONDecoder struct {
	s scanner.Scanner
}

func Newint64JSONScanDecoder(s scanner.Scanner) *int64JSONDecoder {
	return &int64JSONDecoder{s: s}
}

func (e *radDiskNodeJSONDecoder) Decode(ptr **radDiskNode) error {
	s := e.s
	if tok, tokval, err := s.Scan(); err != nil {
		return err
	} else if tok == scanner.TNULL {
		*ptr = nil
		return nil
	} else if tok != scanner.TLBRACE {
		return fmt.Errorf("Unexpected %s at %d: %s; expected '{'", scanner.TokenName(tok), s.Pos(), string(tokval))
	}

	// Create the object if it doesn't exist.
	if *ptr == nil {
		*ptr = &radDiskNode{}
	}
	v := *ptr

	// Loop over key/value pairs.
	index := 0
	for {
		// Read in key.
		var key string
		tok, tokval, err := s.Scan()
		if err != nil {
			return err
		} else if tok == scanner.TRBRACE {
			return nil
		} else if tok == scanner.TCOMMA {
			if index == 0 {
				return fmt.Errorf("Unexpected comma at %d", s.Pos())
			}
			if tok, tokval, err = s.Scan(); err != nil {
				return err
			}
		}

		if tok != scanner.TSTRING {
			return fmt.Errorf("Unexpected %s at %d: %s; expected '{' or string", scanner.TokenName(tok), s.Pos(), string(tokval))
		} else {
			key = string(tokval)
		}

		// Read in the colon.
		if tok, tokval, err := s.Scan(); err != nil {
			return err
		} else if tok != scanner.TCOLON {
			return fmt.Errorf("Unexpected %s at %d: %s; expected colon", scanner.TokenName(tok), s.Pos(), string(tokval))
		}

		switch key {

		case "p":
			v := &v.Prefix

			if err := s.ReadString(v); err != nil {
				return err
			}

		case "c":
			v := &v.Children

			if err := Newint64JSONScanDecoder(s).DecodeArray(v); err != nil {
				return err
			}

		case "val":
			v := &v.Value

			if err := s.ReadString(v); err != nil {
				return err
			}

		case "ver":
			v := &v.Version

			if err := s.ReadInt64(v); err != nil {
				return err
			}

		case "seq":
			v := &v.Seq

			if err := s.ReadInt64(v); err != nil {
				return err
			}

		}

		index++
	}

	return nil
}

func (e *int64JSONDecoder) DecodeArray(ptr *[]int64) error {
	s := e.s
	if tok, _, err := s.Scan(); err != nil {
		return err
	} else if tok != scanner.TLBRACKET {
		return errors.New("Expected '['")
	}

	slice := make([]int64, 0)

	// Loop over items.
	index := 0
	for {
		tok, tokval, err := s.Scan()
		if err != nil {
			return err
		} else if tok == scanner.TRBRACKET {
			*ptr = slice
			return nil
		} else if tok == scanner.TCOMMA {
			if index == 0 {
				return fmt.Errorf("Unexpected comma in array at %d", s.Pos())
			}
			if tok, tokval, err = s.Scan(); err != nil {
				return err
			}
		}
		// s.Unscan(tok, tokval)

		// println(string(tokval))

		item, err := strconv.Atoi(string(tokval))
		if err != nil {
			return err
		}
		slice = append(slice, int64(item))

		index++
	}
}

func (e *radDiskNodeJSONDecoder) DecodeArray(ptr *[]*radDiskNode) error {
	s := e.s
	if tok, _, err := s.Scan(); err != nil {
		return err
	} else if tok != scanner.TLBRACKET {
		return errors.New("Expected '['")
	}

	slice := make([]*radDiskNode, 0)

	// Loop over items.
	index := 0
	for {
		tok, tokval, err := s.Scan()
		if err != nil {
			return err
		} else if tok == scanner.TRBRACKET {
			*ptr = slice
			return nil
		} else if tok == scanner.TCOMMA {
			if index == 0 {
				return fmt.Errorf("Unexpected comma in array at %d", s.Pos())
			}
			if tok, tokval, err = s.Scan(); err != nil {
				return err
			}
		}
		s.Unscan(tok, tokval)

		item := &radDiskNode{}
		if err := e.Decode(&item); err != nil {
			return err
		}
		slice = append(slice, item)

		index++
	}
}
