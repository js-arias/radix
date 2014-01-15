package radix

import (
	"github.com/ngaut/megajson/encoder"
	"io"
)

type radDiskNodeJSONEncoder struct {
	w io.Writer
}

func NewradDiskNodeJSONEncoder(w io.Writer) *radDiskNodeJSONEncoder {
	return &radDiskNodeJSONEncoder{w: w}
}

func (e *radDiskNodeJSONEncoder) Encode(v *radDiskNode) error {
	if v == nil {
		return encoder.WriteBytes(e.w, []byte(`null`))
	}

	if err := encoder.WriteByte(e.w, '{'); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, "p"); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ':'); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, v.Prefix); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ','); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, "c"); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ':'); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, '['); err != nil {
		return err
	}
	for index, elem := range v.Children {
		if index > 0 {
			if err := encoder.WriteByte(e.w, ','); err != nil {
				return err
			}
		}
		if err := encoder.WriteInt64(e.w, elem); err != nil {
			return err
		}
	}
	if err := encoder.WriteByte(e.w, ']'); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ','); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, "val"); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ':'); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, v.Value); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ','); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, "ver"); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ':'); err != nil {
		return err
	}
	if err := encoder.WriteInt64(e.w, v.Version); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ','); err != nil {
		return err
	}
	if err := encoder.WriteString(e.w, "seq"); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, ':'); err != nil {
		return err
	}
	if err := encoder.WriteInt64(e.w, v.Seq); err != nil {
		return err
	}
	if err := encoder.WriteByte(e.w, '}'); err != nil {
		return err
	}
	return nil
}
