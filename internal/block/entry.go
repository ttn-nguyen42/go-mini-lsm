package block

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type Entry struct {
	Key   types.Bytes
	Value types.Bytes
	Size  int
}

func (e Entry) String() string {
	sb := strings.Builder{}
	sb.WriteString("key=")
	sb.Write(e.Key)
	sb.WriteString(":")
	sb.WriteString("val=")
	sb.Write(e.Value)

	return sb.String()
}

// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------
func (e entry) encode() []byte {
	buf := make([]byte, e.size())
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], e.keyLen)
	offset += 2

	copy(buf[offset:], e.key)
	offset += int(e.keyLen)

	binary.BigEndian.PutUint16(buf[offset:], e.valueLen)
	offset += 2

	copy(buf[offset:], e.value)
	offset += int(e.valueLen)

	return buf
}

func (e *entry) decode(data []byte) error {
	offset := 0
	if len(data) < 2 {
		return fmt.Errorf("data too short for keyLen")
	}
	e.keyLen = binary.BigEndian.Uint16(data)
	offset += 2

	if len(data) < offset+int(e.keyLen)+2 {
		return fmt.Errorf("data too short for key")
	}
	e.key = make([]byte, e.keyLen)
	copy(e.key, data[offset:offset+int(e.keyLen)])
	offset += int(e.keyLen)

	if len(data) < offset+2 {
		return fmt.Errorf("data too short for valueLen")
	}
	e.valueLen = binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if len(data) < offset+int(e.valueLen) {
		return fmt.Errorf("data too short for value")
	}
	e.value = make([]byte, e.valueLen)
	copy(e.value, data[offset:offset+int(e.valueLen)])
	offset += int(e.valueLen)

	if offset != len(data) {
		return fmt.Errorf("data has extra bytes: expected %d, got %d", offset, len(data))
	}

	return nil
}

func getEntry(key types.Bytes, value types.Bytes) *entry {
	if len(key) > 65535 {
		panic("key size too large")
	}

	if len(value) > 65535 {
		panic("value size too large")
	}

	e := &entry{
		key:      key,
		value:    value,
		keyLen:   uint16(key.Size()),
		valueLen: uint16(value.Size()),
	}

	return e
}
