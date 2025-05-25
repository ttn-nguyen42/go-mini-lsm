package block

import (
	"fmt"
	"strings"
)

var ErrBlockEmpty error = fmt.Errorf("block empty")

type Block struct {
	data    []byte
	offsets []uint16
	size    int
}

func (b *Block) String() string {
	sb := strings.Builder{}
	sb.WriteString("data=")
	sb.Write(b.data)

	sb.WriteString("offsets=")
	sb.WriteString(fmt.Sprintf("%v", b.offsets))

	sb.WriteString("size=")
	sb.WriteString(fmt.Sprintf("%d", b.size))

	return sb.String()
}

func (b *Block) Entries() []Entry {
	entries := make([]Entry, 0, len(b.offsets))

	for i := 0; i < len(b.offsets); i += 1 {
		entryStart := b.offsets[i]

		var entryStop uint16 = 0
		if i >= len(b.offsets)-1 {
			entryStop = uint16(len(b.data))
		} else {
			entryStop = b.offsets[i+1]
		}

		entry := &entry{}
		entry.decode(b.data[entryStart:entryStop])

		entries = append(entries, Entry{
			Key:   entry.key,
			Value: entry.value,
			Size:  entry.size(),
		})
	}

	return entries
}

func (b *Block) First() (Entry, error) {
	e, err := b.first()
	if err != nil {
		return Entry{}, nil
	}

	return Entry{Key: e.key, Value: e.value, Size: e.size()}, nil
}

func (b *Block) first() (*entry, error) {
	var e entry
	if len(b.offsets) == 0 {
		return nil, ErrBlockEmpty
	}

	first := b.offsets[0]
	end := len(b.data)
	if len(b.offsets) > 0 {
		end = int(b.offsets[1])
	}

	if err := e.decode(b.data[first:end]); err != nil {
		return nil, err
	}

	return &e, nil
}

func (b *Block) Scan() Iterator {
	return newIter(b)
}
