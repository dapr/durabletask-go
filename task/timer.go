package task

type CreateTimerOption func(*createTimerOptions) error

type createTimerOptions struct {
	name *string
}

func WithTimerName(name string) CreateTimerOption {
	return func(opt *createTimerOptions) error {
		opt.name = &name
		return nil
	}
}
