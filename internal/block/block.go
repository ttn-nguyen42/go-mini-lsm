package block

import (
	"fmt"
	"strings"
)

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
