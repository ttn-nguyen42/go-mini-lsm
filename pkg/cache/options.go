package cache

type Options struct {
	evictHook EvictionHook
}

type Option func(*Options)

type EvictionHook func(key any, value any, ts int64)

func WithEvictHook(h EvictionHook) Option {
	return func(o *Options) {
		o.evictHook = h
	}
}

func newOptions(options ...Option) *Options {
	opts := &Options{}
	for _, opt := range options {
		opt(opts)
	}

	return opts
}
