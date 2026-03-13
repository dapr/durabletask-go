package task

import (
	"testing"
	"time"
)

func Test_computeNextDelay(t *testing.T) {
	time1 := time.Now()
	time2 := time.Now().Add(1 * time.Minute)
	type args struct {
		currentTimeUtc time.Time
		policy         RetryPolicy
		attempt        int
		firstAttempt   time.Time
		err            error
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "first attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      0,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
		{
			name: "second attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      1,
				firstAttempt: time1,
			},
			want: 4 * time.Second,
		},
		{
			name: "third attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      2,
				firstAttempt: time1,
			},
			want: 8 * time.Second,
		},
		{
			name: "fourth attempt",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 10 * time.Second,
		},
		{
			name: "expired",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   2,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         30 * time.Second,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 0,
		},
		{
			name: "fourth attempt backoff 1",
			args: args{
				currentTimeUtc: time2,
				policy: RetryPolicy{
					MaxAttempts:          3,
					InitialRetryInterval: 2 * time.Second,
					BackoffCoefficient:   1,
					MaxRetryInterval:     10 * time.Second,
					Handle:               func(err error) bool { return true },
					RetryTimeout:         2 * time.Minute,
				},
				attempt:      3,
				firstAttempt: time1,
			},
			want: 2 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := computeNextDelay(tt.args.currentTimeUtc, tt.args.policy, tt.args.attempt, tt.args.firstAttempt, tt.args.err); got != tt.want {
				t.Errorf("computeNextDelay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_computeNextDelay_jitter(t *testing.T) {
	firstAttempt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	currentTime := firstAttempt.Add(1 * time.Minute)

	basePolicy := RetryPolicy{
		MaxAttempts:          5,
		InitialRetryInterval: 2 * time.Second,
		BackoffCoefficient:   2,
		MaxRetryInterval:     30 * time.Second,
		Handle:               func(err error) bool { return true },
		RetryTimeout:         10 * time.Minute,
	}

	t.Run("jitter reduces delay", func(t *testing.T) {
		policy := basePolicy
		policy.JitterFactor = 0.5

		for attempt := 0; attempt < 4; attempt++ {
			withoutJitter := computeNextDelay(currentTime, basePolicy, attempt, firstAttempt, nil)
			withJitter := computeNextDelay(currentTime, policy, attempt, firstAttempt, nil)

			if withJitter >= withoutJitter {
				t.Errorf("attempt %d: jitter delay %v should be less than base delay %v", attempt, withJitter, withoutJitter)
			}
			if withJitter <= 0 {
				t.Errorf("attempt %d: jitter delay should be positive, got %v", attempt, withJitter)
			}
		}
	})

	t.Run("jitter is deterministic across replays", func(t *testing.T) {
		policy := basePolicy
		policy.JitterFactor = 0.8

		for attempt := 0; attempt < 4; attempt++ {
			d1 := computeNextDelay(currentTime, policy, attempt, firstAttempt, nil)
			d2 := computeNextDelay(currentTime, policy, attempt, firstAttempt, nil)
			if d1 != d2 {
				t.Errorf("attempt %d: replay produced different delays: %v vs %v", attempt, d1, d2)
			}
		}
	})

	t.Run("zero jitter factor produces no jitter", func(t *testing.T) {
		policy := basePolicy
		policy.JitterFactor = 0

		for attempt := 0; attempt < 4; attempt++ {
			withJitter := computeNextDelay(currentTime, policy, attempt, firstAttempt, nil)
			withoutJitter := computeNextDelay(currentTime, basePolicy, attempt, firstAttempt, nil)
			if withJitter != withoutJitter {
				t.Errorf("attempt %d: zero jitter should equal base delay: %v vs %v", attempt, withJitter, withoutJitter)
			}
		}
	})

	t.Run("different attempts produce different delays", func(t *testing.T) {
		policy := basePolicy
		policy.JitterFactor = 0.5

		delays := make(map[time.Duration]bool)
		for attempt := 0; attempt < 4; attempt++ {
			d := computeNextDelay(currentTime, policy, attempt, firstAttempt, nil)
			delays[d] = true
		}
		if len(delays) < 2 {
			t.Errorf("expected different delays across attempts, got %d unique values", len(delays))
		}
	})

	t.Run("jitter respects max retry interval", func(t *testing.T) {
		policy := basePolicy
		policy.JitterFactor = 0.5
		policy.MaxRetryInterval = 5 * time.Second

		// attempt 3: base = 2s * 2^3 = 16s, capped to 5s, then jitter applied
		d := computeNextDelay(currentTime, policy, 3, firstAttempt, nil)
		if d > 5*time.Second {
			t.Errorf("delay %v should not exceed MaxRetryInterval 5s", d)
		}
		if d <= 0 {
			t.Errorf("delay should be positive, got %v", d)
		}
	})
}

func Test_RetryPolicy_Validate_JitterFactor(t *testing.T) {
	t.Run("negative jitter clamped to zero", func(t *testing.T) {
		p := RetryPolicy{
			InitialRetryInterval: 1 * time.Second,
			JitterFactor:         -0.5,
		}
		p.Validate()
		if p.JitterFactor != 0 {
			t.Errorf("expected 0, got %f", p.JitterFactor)
		}
	})

	t.Run("jitter above 1 clamped to 1", func(t *testing.T) {
		p := RetryPolicy{
			InitialRetryInterval: 1 * time.Second,
			JitterFactor:         1.5,
		}
		p.Validate()
		if p.JitterFactor != 1 {
			t.Errorf("expected 1, got %f", p.JitterFactor)
		}
	})

	t.Run("valid jitter unchanged", func(t *testing.T) {
		p := RetryPolicy{
			InitialRetryInterval: 1 * time.Second,
			JitterFactor:         0.7,
		}
		p.Validate()
		if p.JitterFactor != 0.7 {
			t.Errorf("expected 0.7, got %f", p.JitterFactor)
		}
	})
}
