package block

type BuilderOptions struct {
	BlockSize uint32
}

type BuilderOption func(opts *BuilderOptions)

func getBuilderOpts(options ...BuilderOption) *BuilderOptions {
	defOpts := &BuilderOptions{
		BlockSize: 4096,
	}

	for _, opt := range options {
		opt(defOpts)
	}

	return defOpts
}

func WithBlockSize(size uint32) BuilderOption {
	return func(opts *BuilderOptions) {
		opts.BlockSize = size
	}
}
