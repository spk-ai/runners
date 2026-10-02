package rpcauth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestCache(fake *fakeTokenReviews, ttl time.Duration, clock *fakeClock) *cachingReviewer {
	c := newCachingReviewer(NewReviewer(fake, testAudience, time.Second), ttl)
	c.now = clock.Now
	return c
}

func TestCacheReusesSuccessUntilTTL(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	token := testJWT("gateway", clock.now.Add(10*time.Minute))
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	cache := newTestCache(fake, 30*time.Second, clock)
	for i := 0; i < 3; i++ {
		if _, err := cache.Review(context.Background(), token); err != nil {
			t.Fatalf("review: %v", err)
		}
	}
	if fake.callCount() != 1 {
		t.Fatalf("reviews %d within TTL, want 1", fake.callCount())
	}
	clock.Advance(29 * time.Second)
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 1 {
		t.Fatal("cache expired early")
	}
	clock.Advance(time.Second)
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 2 {
		t.Fatalf("reviews %d after TTL, want 2", fake.callCount())
	}
}

func TestCacheNeverOutlivesTokenExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	token := testJWT("gateway", clock.now.Add(12*time.Second))
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	cache := newTestCache(fake, 60*time.Second, clock)
	_, _ = cache.Review(context.Background(), token)
	clock.Advance(6 * time.Second)
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 1 {
		t.Fatal("cache missed before exp-5s")
	}
	clock.Advance(time.Second) // exp - 5s
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 2 {
		t.Fatalf("reviews %d at exp-5s, want 2", fake.callCount())
	}
}

func TestCacheSkipsTokensWithoutExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	token := "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln" // no exp claim
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	cache := newTestCache(fake, 30*time.Second, clock)
	_, _ = cache.Review(context.Background(), token)
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 2 {
		t.Fatalf("reviews %d, want no positive caching without exp", fake.callCount())
	}
}

func TestCacheNegativeResultsBriefly(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	token := testJWT("forged", clock.now.Add(time.Hour))
	cache := newTestCache(fake, 30*time.Second, clock)
	for i := 0; i < 2; i++ {
		if _, err := cache.Review(context.Background(), token); !errors.Is(err, errNotAuthenticated) {
			t.Fatalf("error %v, want rejection", err)
		}
	}
	if fake.callCount() != 1 {
		t.Fatalf("reviews %d, want negative cache hit", fake.callCount())
	}
	clock.Advance(negativeCacheTTL)
	_, _ = cache.Review(context.Background(), token)
	if fake.callCount() != 2 {
		t.Fatal("negative cache outlived 5s")
	}
}

func TestCacheNeverStoresTransientErrors(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	token := testJWT("gateway", clock.now.Add(time.Hour))
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	fake.setError(errors.New("connection refused"))
	cache := newTestCache(fake, 30*time.Second, clock)
	if _, err := cache.Review(context.Background(), token); err == nil || errors.Is(err, errNotAuthenticated) {
		t.Fatalf("error %v, want transient", err)
	}
	fake.setError(nil)
	if _, err := cache.Review(context.Background(), token); err != nil {
		t.Fatalf("recovered review: %v", err)
	}
	if fake.callCount() != 2 {
		t.Fatalf("reviews %d, want the transient error uncached", fake.callCount())
	}
}

func TestCacheSharesConcurrentIdenticalReviews(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	fake.block = make(chan struct{})
	token := testJWT("gateway", clock.now.Add(time.Hour))
	fake.statuses[token] = serviceAccountStatus(testNamespace, "gateway", testAudience)
	cache := newTestCache(fake, 30*time.Second, clock)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.Review(context.Background(), token)
			errs <- err
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for fake.callCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(fake.block)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("shared review: %v", err)
		}
	}
	if fake.callCount() != 1 {
		t.Fatalf("reviews %d, want 1 shared", fake.callCount())
	}
}

func TestCacheBoundsConcurrentReviews(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	fake := newFakeTokenReviews()
	fake.block = make(chan struct{})
	defer close(fake.block)
	cache := newTestCache(fake, 30*time.Second, clock)
	cache.wait = 20 * time.Millisecond
	for i := 0; i < maxInFlightReviews; i++ {
		go func(i int) {
			_, _ = cache.Review(context.Background(), testJWT(string(rune('a'+i)), clock.now.Add(time.Hour)))
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for fake.callCount() < maxInFlightReviews && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	_, err := cache.Review(context.Background(), testJWT("overflow", clock.now.Add(time.Hour)))
	if !errors.Is(err, errReviewBusy) {
		t.Fatalf("error %v, want busy", err)
	}
}

func TestUnverifiedExpiry(t *testing.T) {
	exp := time.Unix(1_800_000_600, 0)
	if got, ok := unverifiedExpiry(testJWT("x", exp)); !ok || !got.Equal(exp) {
		t.Fatalf("exp %v %v", got, ok)
	}
	for _, token := range []string{"", "a.b", "a.!!!.c", "eyJhbGciOiJSUzI1NiJ9.bm90LWpzb24.c2ln", "eyJhbGciOiJSUzI1NiJ9.eyJleHAiOiJzb29uIn0.c2ln"} {
		if _, ok := unverifiedExpiry(token); ok {
			t.Errorf("expiry read from %q", token)
		}
	}
}
