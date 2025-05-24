package lsm

type Options struct {
	MaxTableSize int
}

type Option func(*Options)

func getOptions(opts ...Option) *Options {
	o := &Options{
		MaxTableSize: 256 * 1024 * 1024,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}
