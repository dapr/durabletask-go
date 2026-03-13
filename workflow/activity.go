package workflow

import (
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/task"
)

type CallActivityOption task.CallActivityOption

type RetryPolicy struct {
	// Max number of attempts to try the activity call, first execution inclusive
	MaxAttempts int
	// Timespan to wait for the first retry
	InitialRetryInterval time.Duration
	// Used to determine rate of increase of back-off
	BackoffCoefficient float64
	// Max timespan to wait for a retry
	MaxRetryInterval time.Duration
	// Total timeout across all the retries performed
	RetryTimeout time.Duration
	// Optional function to control if retries should proceed
	Handle func(error) bool
	// JitterFactor adds randomness to retry delays to desynchronize concurrent retries.
	// Must be in [0.0, 1.0]: 0.0 disables jitter, 1.0 allows up to 100% reduction of the delay.
	// The jitter is deterministic across orchestrator replays (seeded by firstAttempt + attempt).
	JitterFactor float64
}

func WithActivityAppID(targetAppID string) CallActivityOption {
	return CallActivityOption(task.WithActivityAppID(targetAppID))
}

// WithActivityInput configures an input for an activity invocation. The
// specified input must be JSON serializable.
func WithActivityInput(input any) CallActivityOption {
	return CallActivityOption(task.WithActivityInput(input))
}

// WithRawActivityInput configures a raw input for an activity invocation.
func WithRawActivityInput(input *wrapperspb.StringValue) CallActivityOption {
	return CallActivityOption(task.WithRawActivityInput(input))
}

func WithActivityRetryPolicy(policy *RetryPolicy) CallActivityOption {
	if policy == nil {
		return CallActivityOption(task.WithActivityRetryPolicy(nil))
	}

	return CallActivityOption(task.WithActivityRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          policy.MaxAttempts,
		InitialRetryInterval: policy.InitialRetryInterval,
		BackoffCoefficient:   policy.BackoffCoefficient,
		MaxRetryInterval:     policy.MaxRetryInterval,
		RetryTimeout:         policy.RetryTimeout,
		Handle:               policy.Handle,
		JitterFactor:         policy.JitterFactor,
	}))
}

// ActivityContext is the context parameter type for activity implementations.
type ActivityContext task.ActivityContext

// Activity is the functional interface for activity implementations.
type Activity func(ActivityContext) (any, error)
