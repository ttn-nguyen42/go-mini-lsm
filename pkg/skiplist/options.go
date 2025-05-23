package skiplist

import "fmt"

type Options struct {
	MaxLevel int
}

type Option func(*Options)

func WithMaxLevel(level int) Option {
	return func(o *Options) {
		o.MaxLevel = level
	}
}

func getOptions(opts ...Option) (*Options, error) {
	options := &Options{
		MaxLevel: 20,
	}

	for _, opt := range opts {
		opt(options)
	}

	if options.MaxLevel <= 0 {
		return nil, fmt.Errorf("max level must be greater than 0")
	}

	return options, nil
}
