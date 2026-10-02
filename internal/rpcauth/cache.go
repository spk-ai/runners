package rpcauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	// negativeCacheTTL absorbs retries of a token the API server rejected.
	negativeCacheTTL = 5 * time.Second
	// expirySkew stops reuse slightly before the token's own expiry.
	expirySkew = 5 * time.Second
	// maxCacheEntries bounds memory; at most ~3 callers rotate every ~8 minutes.
	maxCacheEntries = 4096
	// maxInFlightReviews protects an API server on a slow-disk node.
	maxInFlightReviews = 16
	// slotWait is how long a cache miss may queue for a review slot.
	slotWait = time.Second
)

var errReviewBusy = errors.New("too many concurrent token reviews")

type tokenReviewer interface {
	Review(ctx context.Context, token string) (Principal, error)
}

type cacheEntry struct {
	principal Principal
	err       error
	expires   time.Time
}

type reviewResult struct {
	principal Principal
	err       error
}

// cachingReviewer reuses TokenReview outcomes, keyed by the SHA-256 of the
// token so the cache never holds a usable credential. Successes are reused
// for min(policy TTL, token exp - 5s); definitive rejections for 5s; transient
// errors (API unavailable, RBAC missing, throttling, timeouts) are never
// cached and never allow. Identical concurrent misses share one review.
type cachingReviewer struct {
	reviewer tokenReviewer
	ttl      time.Duration
	now      func() time.Time

	mu      sync.Mutex
	entries map[[sha256.Size]byte]cacheEntry
	group   singleflight.Group
	slots   chan struct{}
	wait    time.Duration
}

func newCachingReviewer(reviewer tokenReviewer, ttl time.Duration) *cachingReviewer {
	if ttl <= 0 || ttl > MaxCacheTTL {
		ttl = DefaultCacheTTL
	}
	return &cachingReviewer{
		reviewer: reviewer,
		ttl:      ttl,
		now:      time.Now,
		entries:  map[[sha256.Size]byte]cacheEntry{},
		slots:    make(chan struct{}, maxInFlightReviews),
		wait:     slotWait,
	}
}

func (c *cachingReviewer) Review(ctx context.Context, token string) (Principal, error) {
	key := sha256.Sum256([]byte(token))
	if entry, ok := c.get(key); ok {
		return entry.principal, entry.err
	}
	value, _, _ := c.group.Do(string(key[:]), func() (any, error) {
		if entry, ok := c.get(key); ok {
			return reviewResult{entry.principal, entry.err}, nil
		}
		timer := time.NewTimer(c.wait)
		defer timer.Stop()
		select {
		case c.slots <- struct{}{}:
		case <-timer.C:
			return reviewResult{err: errReviewBusy}, nil
		}
		defer func() { <-c.slots }()
		// Waiters share this review, so one caller's cancellation must not
		// fail the others; the reviewer applies its own deadline.
		principal, err := c.reviewer.Review(context.WithoutCancel(ctx), token)
		c.store(key, token, principal, err)
		return reviewResult{principal, err}, nil
	})
	result := value.(reviewResult)
	return result.principal, result.err
}

func (c *cachingReviewer) get(key [sha256.Size]byte) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return cacheEntry{}, false
	}
	if !c.now().Before(entry.expires) {
		delete(c.entries, key)
		return cacheEntry{}, false
	}
	return entry, true
}

func (c *cachingReviewer) store(key [sha256.Size]byte, token string, principal Principal, err error) {
	now := c.now()
	var expires time.Time
	switch {
	case err == nil:
		exp, ok := unverifiedExpiry(token)
		if !ok {
			// Without a readable expiry the token could outlive any TTL.
			return
		}
		expires = now.Add(c.ttl)
		if limit := exp.Add(-expirySkew); limit.Before(expires) {
			expires = limit
		}
		if !expires.After(now) {
			return
		}
	case errors.Is(err, errNotAuthenticated):
		expires = now.Add(negativeCacheTTL)
	default:
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= maxCacheEntries {
		for k, entry := range c.entries {
			if !now.Before(entry.expires) {
				delete(c.entries, k)
			}
		}
		for k := range c.entries {
			if len(c.entries) < maxCacheEntries {
				break
			}
			delete(c.entries, k)
		}
	}
	c.entries[key] = cacheEntry{principal: principal, err: err, expires: expires}
}

// unverifiedExpiry reads the exp claim. The API server has already verified
// the signature; this only shortens cache lifetime, never extends trust.
func unverifiedExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp *json.Number `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Exp == nil {
		return time.Time{}, false
	}
	seconds, err := claims.Exp.Int64()
	if err != nil || seconds <= 0 {
		return time.Time{}, false
	}
	return time.Unix(seconds, 0), true
}
