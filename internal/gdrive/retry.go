package gdrive

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"
	"google.golang.org/api/googleapi"
)

const (
	gdriveRetryMaxAttempts  = 5
	gdriveRetryBaseDelay    = 1 * time.Second
	gdriveRetryMaxDelay     = 30 * time.Second
	gdriveRequestInterval   = 100 * time.Millisecond
	gdriveRequestBurstLimit = 1
)

var gdriveLimiter = rate.NewLimiter(rate.Every(gdriveRequestInterval), gdriveRequestBurstLimit)

func doGDriveRequest[T any](ctx context.Context, operation string, fn func() (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}

	backoff := gdriveRetryBaseDelay
	var lastErr error
	for attempt := 1; attempt <= gdriveRetryMaxAttempts; attempt++ {
		if err := gdriveLimiter.Wait(ctx); err != nil {
			return zero, err
		}

		value, err := fn()
		if err == nil {
			return value, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		retry, retryAfter := shouldRetryGDrive(err)
		if !retry || attempt == gdriveRetryMaxAttempts {
			return zero, err
		}

		wait := backoff
		if retryAfter > wait {
			wait = retryAfter
		}
		if wait <= 0 {
			wait = gdriveRetryBaseDelay
		}

		log.Warn().
			Err(err).
			Str("operation", operation).
			Int("attempt", attempt).
			Dur("backoff", wait).
			Msg("GDrive request failed; retrying")

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}

		backoff = min(backoff*2, gdriveRetryMaxDelay)
	}

	return zero, lastErr
}

func shouldRetryGDrive(err error) (bool, time.Duration) {
	if err == nil {
		return false, 0
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, 0
	}

	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		retryAfter := retryAfterDuration(apiErr.Header)
		if isRetryableStatus(apiErr.Code) || isRetryableReason(apiErr.Errors) {
			return true, retryAfter
		}
		return false, 0
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true, 0
	}

	return false, 0
}

func retryAfterDuration(header http.Header) time.Duration {
	if header == nil {
		return 0
	}
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		delay := time.Until(t)
		if delay > 0 {
			return delay
		}
	}
	return 0
}

func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func isRetryableReason(items []googleapi.ErrorItem) bool {
	for _, item := range items {
		switch strings.ToLower(strings.TrimSpace(item.Reason)) {
		case "ratelimitexceeded",
			"userratelimitexceeded",
			"quotaexceeded",
			"dailylimitexceeded",
			"backenderror",
			"internalerror":
			return true
		default:
		}
	}
	return false
}
