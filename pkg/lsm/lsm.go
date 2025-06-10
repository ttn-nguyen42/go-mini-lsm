package lsm

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type LSM interface {
	Put(key types.Bytes, value types.Bytes)
	Delete(key types.Bytes)
	Get(key types.Bytes) (types.Bytes, bool)
	Sync()
	Scan(lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator
	Transaction()
}

type lsm struct {
	state sync.Mutex
	rw    sync.RWMutex

	opts *Options

	memTableId atomic.Int32
	sstId      atomic.Int32

	currTable   memtable.MemTable
	immutTables []memtable.MemTable

	l0SsTables []sst.SortedTable
	sstLevels  [][]int32
	ssTables   map[int32][]sst.SortedTable

	iterCount int
}

func New(options ...Option) LSM {
	return &lsm{
		opts:        getOptions(options...),
		memTableId:  atomic.Int32{},
		sstId:       atomic.Int32{},
		immutTables: make([]memtable.MemTable, 0),
		currTable:   memtable.New(0),
		l0SsTables:  make([]sst.SortedTable, 0),
		state:       sync.Mutex{},
		rw:          sync.RWMutex{},
		iterCount:   0,
		sstLevels:   make([][]int32, 0, 3),
		ssTables:    make(map[int32][]sst.SortedTable),
	}
}

func (m *lsm) Delete(key types.Bytes) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	m.markDeleted(key)
}

func (m *lsm) markDeleted(key types.Bytes) {
	m.currTable.Put(key, make(types.Bytes, 0))
}

func (m *lsm) Get(key types.Bytes) (types.Bytes, bool) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.get(key)
}

func (m *lsm) get(key types.Bytes) (types.Bytes, bool) {
	if val, found := m.getFromTable(m.currTable, key); found {
		return val, true
	}

	if val, found := m.getFromPastTables(key); found {
		return val, true
	}

	return nil, false
}

func (m *lsm) getFromTable(table memtable.MemTable, key types.Bytes) (types.Bytes, bool) {
	val, found := table.Get(key)
	if !found {
		return nil, false
	}

	if len(val) == 0 {
		return nil, false
	}

	return val, true
}

func (m *lsm) getFromPastTables(key types.Bytes) (types.Bytes, bool) {
	for _, table := range m.immutTables {
		if val, found := m.getFromTable(table, key); found {
			return val, true
		}
	}

	return nil, false
}

func (m *lsm) Put(key types.Bytes, value types.Bytes) {
	m.rw.RLock()

	m.currTable.Put(key, value)
	curSize := m.currTable.Size()
	m.rw.RUnlock()

	m.tryFreeze(curSize)
}

func (m *lsm) tryFreeze(tableSize int) {
	if tableSize >= m.opts.MaxTableSize {
		// only one thread should be freezing memtable
		m.state.Lock()
		defer m.state.Unlock()

		m.rw.RLock()
		// check again some thread already freezed last memtable
		shouldLock := m.currTable.Size() >= m.opts.MaxTableSize
		m.rw.RUnlock()

		if !shouldLock {
			return
		}

		// 2 separate mutexes because
		// - State mutex does not affect other read LSM requests
		// - State mutex make sure one thread should be freezing memtable at once, or else empty ones will be created
		// - Use write lock here, it will unnecessary block read requests, we are not modifing anything
		// - Use write lock when it's time to actually swap out the new memtable
		m.freeze()
	}
}

func (m *lsm) freeze() {
	mt := memtable.New(int(m.memTableId.Add(1)))
	m.rw.Lock()
	defer m.rw.Unlock()

	m.immutTables = append(m.immutTables, m.currTable)
	m.currTable = mt

	log.Printf("Memtable %d frozen, total immutable tables: %d", m.currTable.Id(), len(m.immutTables))
}

func (m *lsm) Sync() {
	panic("unimplemented")
}

func (m *lsm) Transaction() {
	panic("unimplemented")
}

func (m *lsm) Scan(lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.scan(lower, upper)
}

func (m *lsm) scan(lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator {
	memTables := make([]memtable.MemTable, 0, len(m.immutTables)+1)

	memTables = append(memTables, m.immutTables...)
	memTables = append(memTables, m.currTable)

	tablesByLevel := make([][]sst.SortedTable, 0, len(m.sstLevels))
	for _, lvlTableIds := range m.sstLevels {
		tableOnLvl := make([]sst.SortedTable, 0, len(lvlTableIds))

		for _, id := range lvlTableIds {
			table := m.ssTables[id]
			tableOnLvl = append(tableOnLvl, table...)
		}

		tablesByLevel = append(tablesByLevel, tableOnLvl)
	}

	return NewIter(memTables, m.l0SsTables, tablesByLevel, lower, upper)
}
