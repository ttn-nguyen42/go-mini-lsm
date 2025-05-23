package lsm

import (
	"sync"
	"sync/atomic"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
)

type LSM interface {
	Put(key types.Bytes, value types.Bytes)
	Delete(key types.Bytes)
	Get(key types.Bytes) (types.Bytes, bool)
	Sync()
	Transaction()
}

type miniLsm struct {
	state sync.Mutex
	rw    sync.RWMutex

	opts *Options

	memTableId  atomic.Int32
	currTable   memtable.MemTable
	immutTables []memtable.MemTable
}

func New(options ...Option) LSM {
	return &miniLsm{
		opts:        getOptions(options...),
		memTableId:  atomic.Int32{},
		immutTables: make([]memtable.MemTable, 0),
		state:       sync.Mutex{},
		rw:          sync.RWMutex{},
		currTable:   memtable.New(0),
	}
}

func (m *miniLsm) Delete(key types.Bytes) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	m.markDeleted(key)
}

func (m *miniLsm) markDeleted(key types.Bytes) {
	m.currTable.Put(key, make(types.Bytes, 0))
}

func (m *miniLsm) Get(key types.Bytes) (types.Bytes, bool) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.get(key)
}

func (m *miniLsm) get(key types.Bytes) (types.Bytes, bool) {
	if val, found := m.getFromTable(m.currTable, key); found {
		return val, true
	}

	if val, found := m.getFromPastTables(key); found {
		return val, true
	}

	return nil, false
}

func (m *miniLsm) getFromTable(table memtable.MemTable, key types.Bytes) (types.Bytes, bool) {
	val, found := table.Get(key)
	if !found {
		return nil, false
	}

	if len(val) == 0 {
		return nil, false
	}

	return val, true
}

func (m *miniLsm) getFromPastTables(key types.Bytes) (types.Bytes, bool) {
	for _, table := range m.immutTables {
		if val, found := m.getFromTable(table, key); found {
			return val, true
		}
	}

	return nil, false
}

func (m *miniLsm) Put(key types.Bytes, value types.Bytes) {
	m.rw.RLock()

	m.currTable.Put(key, value)
	curSize := m.currTable.Size()
	m.rw.RUnlock()

	m.tryFreeze(curSize)
}

func (m *miniLsm) tryFreeze(tableSize int) {
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

		// we use 2 separate mutexes because
		// - state mutex does not affect other read requests off LSM
		// - state mutex make sure one thread should be freezing memtable at once, or else empty ones will be created
		// - use rw locks to make sure we dont unnecessary block read requests while checking table sizes
		// - use rw write lock when it's time we actually swap out the new memtable
		m.freeze()
	}
}

func (m *miniLsm) freeze() {
	mt := memtable.New(int(m.memTableId.Add(1)))
	m.rw.Lock()
	defer m.rw.Unlock()

	m.immutTables = append(m.immutTables, m.currTable)
	m.currTable = mt
}

func (m *miniLsm) Sync() {
	panic("unimplemented")
}

func (m *miniLsm) Transaction() {
	panic("unimplemented")
}
