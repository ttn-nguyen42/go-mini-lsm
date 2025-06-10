package lsm

type Options struct {
	MaxTableSize   int
	Dir            string
	SstLevelCount  int
	BlockCacheSize int
}

type Option func(*Options)

func getOptions(opts ...Option) *Options {
	o := &Options{
		MaxTableSize:   256 * 1024 * 1024,
		Dir:            "/tmp/mini_lsm",
		SstLevelCount:  3,
		BlockCacheSize: 1 << 20, //4GB
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

func MaxTableSize(maxSize int) Option {
	return func(o *Options) {
		o.MaxTableSize = maxSize
	}
}

func Dir(dir string) Option {
	return func(o *Options) {
		o.Dir = dir
	}
}

func LevelCount(count int) Option {
	return func(o *Options) {
		o.SstLevelCount = count
	}
}

func BlockCacheSize(size int) Option {
	return func(o *Options) {
		o.BlockCacheSize = size
	}
}
