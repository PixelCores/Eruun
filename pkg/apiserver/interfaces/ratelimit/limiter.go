package ratelimit

import "golang.org/x/time/rate"

// Limiter shares HTTP and gRPC operation-class budgets in one API process.
type Limiter struct {
	expensive *rate.Limiter
	read      *rate.Limiter
}

func New(qps float64, burst int) *Limiter {
	if qps <= 0 || burst <= 0 {
		return nil
	}
	return &Limiter{
		expensive: rate.NewLimiter(rate.Limit(qps), burst),
		read:      rate.NewLimiter(rate.Limit(qps*5), burst*5),
	}
}

func (l *Limiter) Allow(expensive bool) bool {
	if l == nil {
		return true
	}
	if expensive {
		return l.expensive.Allow()
	}
	return l.read.Allow()
}
