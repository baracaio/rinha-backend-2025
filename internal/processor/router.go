package processor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// CircuitBreakerState represents the state of a circuit breaker
	CircuitBreakerState int32

	CircuitBreaker struct {
		failureThreshold int
		successThreshold int
		timeout          time.Duration

		// Atomic fields
		state        int32 // CircuitBreakerState
		failures     int32
		successes    int32
		lastFailTime int64 // Unix nano

		mu sync.RWMutex
	}
)

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

var (
	ErrAllProcessorsUnavailable = errors.New("all payment processors are unavailable")
	ErrProcessingTimeout        = errors.New("payment processing timeout")
	ErrCircuitBreakerOpen       = errors.New("circuit breaker is open")
)

// CircuitBreaker implements the circuit breaker pattern for payment processors

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold, successThreshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		state:            int32(CircuitClosed),
	}
}

// CanExecute returns true if the circuit breaker allows execution
func (cb *CircuitBreaker) CanExecute() bool {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	switch state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		// Check if we should transition to half-open
		lastFail := atomic.LoadInt64(&cb.lastFailTime)
		if time.Since(time.Unix(0, lastFail)) > cb.timeout {
			// Attempt to transition to half-open
			if atomic.CompareAndSwapInt32(&cb.state, int32(CircuitOpen), int32(CircuitHalfOpen)) {
				atomic.StoreInt32(&cb.successes, 0)
			}
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful operation
func (cb *CircuitBreaker) RecordSuccess() {
	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	switch state {
	case CircuitClosed:
		atomic.StoreInt32(&cb.failures, 0)
	case CircuitHalfOpen:
		successes := atomic.AddInt32(&cb.successes, 1)
		if int(successes) >= cb.successThreshold {
			atomic.StoreInt32(&cb.state, int32(CircuitClosed))
			atomic.StoreInt32(&cb.failures, 0)
			log.Println("Circuit breaker transitioned to CLOSED")
		}
	}
}

// RecordFailure records a failed operation
func (cb *CircuitBreaker) RecordFailure() {
	atomic.StoreInt64(&cb.lastFailTime, time.Now().UnixNano())

	state := CircuitBreakerState(atomic.LoadInt32(&cb.state))

	switch state {
	case CircuitClosed:
		failures := atomic.AddInt32(&cb.failures, 1)
		if int(failures) >= cb.failureThreshold {
			atomic.StoreInt32(&cb.state, int32(CircuitOpen))
			log.Println("Circuit breaker transitioned to OPEN")
		}
	case CircuitHalfOpen:
		atomic.StoreInt32(&cb.state, int32(CircuitOpen))
		atomic.StoreInt32(&cb.failures, 1)
		log.Println("Circuit breaker transitioned back to OPEN")
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	return CircuitBreakerState(atomic.LoadInt32(&cb.state))
}

// SmartRouter intelligently routes payments between processors
type SmartRouter struct {
	defaultClient  *Client
	fallbackClient *Client
	healthMonitor  *HealthMonitor

	// Circuit breakers for each processor
	defaultCB  *CircuitBreaker
	fallbackCB *CircuitBreaker

	// Metrics
	mu                  sync.RWMutex
	defaultRequests     int64
	defaultSuccesses    int64
	fallbackRequests    int64
	fallbackSuccesses   int64
	totalProcessingTime time.Duration
	totalRequests       int64
}

// NewSmartRouter creates a new smart router
func NewSmartRouter(defaultClient, fallbackClient *Client, healthMonitor *HealthMonitor) *SmartRouter {
	return &SmartRouter{
		defaultClient:  defaultClient,
		fallbackClient: fallbackClient,
		healthMonitor:  healthMonitor,
		defaultCB:      NewCircuitBreaker(5, 3, 30*time.Second),  // 5 failures, 3 successes, 30s timeout
		fallbackCB:     NewCircuitBreaker(10, 5, 60*time.Second), // 10 failures, 5 successes, 60s timeout
	}
}

// RoutePayment intelligently routes a payment to the best available processor
func (sr *SmartRouter) RoutePayment(ctx context.Context, correlationID string, amount float64) (string, error) {
	startTime := time.Now()
	defer func() {
		sr.mu.Lock()
		sr.totalProcessingTime += time.Since(startTime)
		sr.totalRequests++
		sr.mu.Unlock()
	}()

	// Try default processor first (lower fees)
	if sr.shouldUseDefault() {
		if processor, err := sr.tryProcessor(ctx, sr.defaultClient, "default", sr.defaultCB, correlationID, amount); err == nil {
			atomic.AddInt64(&sr.defaultRequests, 1)
			atomic.AddInt64(&sr.defaultSuccesses, 1)
			return processor, nil
		}
		atomic.AddInt64(&sr.defaultRequests, 1)
	}

	// Try fallback processor
	if sr.shouldUseFallback() {
		if processor, err := sr.tryProcessor(ctx, sr.fallbackClient, "fallback", sr.fallbackCB, correlationID, amount); err == nil {
			atomic.AddInt64(&sr.fallbackRequests, 1)
			atomic.AddInt64(&sr.fallbackSuccesses, 1)
			return processor, nil
		}
		atomic.AddInt64(&sr.fallbackRequests, 1)
	}

	// todo accumulate unsuccessful request and try again later.
	return "", ErrAllProcessorsUnavailable
}

// shouldUseDefault determines if we should try the default processor
func (sr *SmartRouter) shouldUseDefault() bool {
	// Check circuit breaker
	if !sr.defaultCB.CanExecute() {
		return false
	}

	// Always prefer default (lower fees) if circuit breaker allows
	return true
}

// shouldUseFallback determines if we should try the fallback processor
func (sr *SmartRouter) shouldUseFallback() bool {
	// Check circuit breaker
	if !sr.fallbackCB.CanExecute() {
		return false
	}

	// Use fallback if health monitor indicates it's available
	return sr.healthMonitor.IsFallbackHealthy()
}

// tryProcessor attempts to process a payment with a specific processor
func (sr *SmartRouter) tryProcessor(ctx context.Context, client *Client, processorName string,
	cb *CircuitBreaker, correlationID string, amount float64,
) (string, error) {
	// Implement retry with exponential backoff
	maxRetries := 3
	baseDelay := 100 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			cb.RecordFailure()
			return "", ctx.Err()
		default:
		}

		// Try processing the payment
		err := client.ProcessPayment(ctx, correlationID, amount)
		if err == nil {
			cb.RecordSuccess()
			log.Printf("Payment %s processed successfully by %s processor (attempt %d/%d)",
				correlationID, processorName, attempt+1, maxRetries)
			return processorName, nil
		}

		// Log the error
		log.Printf("Payment %s failed on %s processor (attempt %d/%d): %v",
			correlationID, processorName, attempt+1, maxRetries, err)

		// If this is the last attempt, record the failure and return
		if attempt == maxRetries-1 {
			cb.RecordFailure()
			return "", err
		}

		// Exponential backoff with jitter
		delay := time.Duration(1<<uint(attempt)) * baseDelay
		if delay > 2*time.Second {
			delay = 2 * time.Second
		}

		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			cb.RecordFailure()
			return "", ctx.Err()
		}
	}

	cb.RecordFailure()
	return "", fmt.Errorf("all retry attempts failed for %s processor", processorName)
}

// GetStats returns routing statistics
func (sr *SmartRouter) GetStats() map[string]any {
	sr.mu.RLock()
	defer sr.mu.RUnlock()

	defaultReq := atomic.LoadInt64(&sr.defaultRequests)
	defaultSucc := atomic.LoadInt64(&sr.defaultSuccesses)
	fallbackReq := atomic.LoadInt64(&sr.fallbackRequests)
	fallbackSucc := atomic.LoadInt64(&sr.fallbackSuccesses)

	var defaultSuccessRate, fallbackSuccessRate float64
	if defaultReq > 0 {
		defaultSuccessRate = float64(defaultSucc) / float64(defaultReq) * 100
	}
	if fallbackReq > 0 {
		fallbackSuccessRate = float64(fallbackSucc) / float64(fallbackReq) * 100
	}

	var avgProcessingTime time.Duration
	if sr.totalRequests > 0 {
		avgProcessingTime = sr.totalProcessingTime / time.Duration(sr.totalRequests)
	}

	return map[string]any{
		"default": map[string]any{
			"requests":     defaultReq,
			"successes":    defaultSucc,
			"successRate":  defaultSuccessRate,
			"circuitState": sr.getCircuitStateString(sr.defaultCB.GetState()),
		},
		"fallback": map[string]any{
			"requests":     fallbackReq,
			"successes":    fallbackSucc,
			"successRate":  fallbackSuccessRate,
			"circuitState": sr.getCircuitStateString(sr.fallbackCB.GetState()),
		},
		"overall": map[string]any{
			"totalRequests":     sr.totalRequests,
			"avgProcessingTime": avgProcessingTime.String(),
		},
	}
}

// getCircuitStateString converts circuit breaker state to string
func (sr *SmartRouter) getCircuitStateString(state CircuitBreakerState) string {
	switch state {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// Close cleans up the router resources
func (sr *SmartRouter) Close() {
	// Close client connections
	if sr.defaultClient != nil {
		sr.defaultClient.Close()
	}
	if sr.fallbackClient != nil {
		sr.fallbackClient.Close()
	}
}
