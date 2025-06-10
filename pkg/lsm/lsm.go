package lsm

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/ttn-nguyen42/go-mini-lsm/internal/memtable"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/sst"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/types"
	"github.com/ttn-nguyen42/go-mini-lsm/internal/utils"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/lsm/concat"
)

type LSM interface {
	Put(key types.Bytes, value types.Bytes)
	Delete(key types.Bytes)
	Get(key types.Bytes) (types.Bytes, bool, error)
	Sync()
	Scan(lower types.Bound[types.Bytes], upper types.Bound[types.Bytes]) types.Iterator
	Transaction()
}

type lsm struct {
	state       sync.Mutex
	rw          sync.RWMutex
	opts        *Options
	memTableId  atomic.Int32
	sstId       atomic.Int32
	currTable   memtable.MemTable
	immutTables []memtable.MemTable
	l0SsTables  []sst.SortedTable
	sstLevels   [][]int32
	ssTables    map[int32][]sst.SortedTable
	iterCount   int
	blockCache  sst.BlockCache
}

func New(options ...Option) (LSM, error) {
	lsm := newInit(options...)
	if err := lsm.open(); err != nil {
		return nil, err
	}
	return lsm, nil
}

func newInit(options ...Option) *lsm {
	opts := getOptions(options...)

	return &lsm{
		opts:        opts,
		memTableId:  atomic.Int32{},
		sstId:       atomic.Int32{},
		immutTables: make([]memtable.MemTable, 0),
		currTable:   memtable.New(0),
		l0SsTables:  make([]sst.SortedTable, 0),
		state:       sync.Mutex{},
		rw:          sync.RWMutex{},
		iterCount:   0,
		sstLevels:   make([][]int32, 0, opts.SstLevelCount),
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

func (m *lsm) Get(key types.Bytes) (types.Bytes, bool, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	return m.get(key)
}

func (m *lsm) getFromMemtables(key types.Bytes) (types.Bytes, bool) {
	if val, found := m.currTable.Get(key); found {
		return val, true
	}
	for _, table := range m.immutTables {
		if val, found := table.Get(key); found {
			return val, true
		}
	}
	return nil, false
}

func (m *lsm) getL0Iterators(key types.Bytes) ([]types.Iterator, error) {
	l0iters := make([]types.Iterator, 0, len(m.l0SsTables))
	for _, table := range m.l0SsTables {
		if types.IsWithinRange(table.FirstKey(), table.LastKey(), key, types.BytesComparator) {
			if table.Contains(key) {
				iter, err := table.Scan()
				if err != nil {
					return nil, err
				}
				_ = iter.SeekToKey(key)
				l0iters = append(l0iters, iter)
			}
		}
	}
	return l0iters, nil
}

func (m *lsm) getLevelIterators(key types.Bytes) ([]types.Iterator, error) {
	levelIters := make([]types.Iterator, 0, len(m.sstLevels))
	for _, levelIds := range m.sstLevels {
		levelTables := make([]sst.SortedTable, 0, len(levelIds))
		for _, id := range levelIds {
			for _, table := range m.ssTables[id] {
				if types.IsWithinRange(table.FirstKey(), table.LastKey(), key, types.BytesComparator) {
					if table.Contains(key) {
						levelTables = append(levelTables, table)
					}
				}
			}
		}
		if len(levelTables) > 0 {
			it := concat.NewConcatIter(levelTables)
			_ = it.SeekToKey(key)
			levelIters = append(levelIters, it)
		}
	}
	return levelIters, nil
}

func (m *lsm) get(key types.Bytes) (types.Bytes, bool, error) {
	if val, found := m.getFromMemtables(key); found {
		return val, true, nil
	}

	l0iters, err := m.getL0Iterators(key)
	if err != nil {
		return nil, false, err
	}
	l0MergedIter := types.NewMergeIter(l0iters...)

	levelIters, err := m.getLevelIterators(key)
	if err != nil {
		return nil, false, err
	}
	mergedIter := types.NewTwoWayIter(l0MergedIter, types.NewMergeIter(levelIters...))
	if mergedIter.HasNext() && types.BytesComparator(mergedIter.Key(), key) == 0 {
		return mergedIter.Value(), true, nil
	}

	return nil, false, nil
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

func (m *lsm) open() error {
	if err := utils.ForceDirExists(m.opts.Dir); err != nil {
		return err
	}
	m.blockCache = sst.NewBlockCache(m.opts.BlockCacheSize)

	return nil
}

func (m *lsm) Close() error {
	var err error
	for _, table := range m.l0SsTables {
		if err = table.Close(); err != nil {
			log.Printf("Failed to close Level 0 SSTables: %s", err)
		}
	}
	return err
}
