package sst

import (
	"github.com/ttn-nguyen42/go-mini-lsm/internal/block"
	"github.com/ttn-nguyen42/go-mini-lsm/pkg/cache"
)

type CacheKey struct {
	SstId   int32
	BlockId int32
}

func (c CacheKey) Size() int {
	return 4 + 4
}

func NewCacheKey(sstId int32, blockId int32) CacheKey {
	return CacheKey{SstId: sstId, BlockId: blockId}
}

type BlockCache cache.Cache[CacheKey, *block.Block]

func NewBlockCache(size int) BlockCache {
	return cache.New[CacheKey, *block.Block](size)
}
