package lsm

type Options struct {
	MaxTableSize int
}

type Option func(*Options)

func getOptions(opts ...Option) *Options {
	o := &Options{
		MaxTableSize: 1,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}
