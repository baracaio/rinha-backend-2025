package processor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type (
	// PaymentStatus represents the current status of a payment
	PaymentStatus string

	// QueuedPayment represents a payment in the processing queue
	QueuedPayment struct {
		CorrelationID string        `json:"correlationId"`
		Amount        float64       `json:"amount"`
		RequestedAt   time.Time     `json:"requestedAt"`
		Status        PaymentStatus `json:"status"`
		ProcessedBy   string        `json:"processedBy,omitempty"`

		// Retry tracking
		AttemptCount int       `json:"attemptCount"`
		LastAttempt  time.Time `json:"lastAttempt"`
		NextRetry    time.Time `json:"nextRetry"`
		MaxRetries   int       `json:"maxRetries"`

		// Error tracking
		LastError    string `json:"lastError,omitempty"`
		FailureCount int    `json:"failureCount"`
	}

	// PaymentQueue manages payment processing with retry logic
	PaymentQueue struct {
		router  *SmartRouter
		storage PaymentStorage // Interface for persisting payments

		// In-memory queues
		pendingQueue chan *QueuedPayment
		retryQueue   chan *QueuedPayment

		// Workers
		workers       int
		workerWG      sync.WaitGroup
		retryWorkerWG sync.WaitGroup

		// Control
		ctx     context.Context
		cancel  context.CancelFunc
		stopped bool
		mu      sync.RWMutex
	}

	// PaymentStorage interface for persisting payment state
	PaymentStorage interface {
		SavePayment(payment *QueuedPayment) error
		UpdatePaymentStatus(correlationID string, status PaymentStatus, processedBy string) error
		GetPayment(correlationID string) (*QueuedPayment, error)
		GetFailedPayments() ([]*QueuedPayment, error)
		GetPaymentsSummary(from, to time.Time) (map[string]any, error)
	}
)

const (
	StatusPending    PaymentStatus = "pending"
	StatusProcessing PaymentStatus = "processing"
	StatusProcessed  PaymentStatus = "processed"
	StatusFailed     PaymentStatus = "failed"
	StatusRetrying   PaymentStatus = "retrying"
)

// NewPaymentQueue creates a new payment queue with retry capabilities
func NewPaymentQueue(router *SmartRouter, storage PaymentStorage, workers int) *PaymentQueue {
	ctx, cancel := context.WithCancel(context.Background())

	return &PaymentQueue{
		router:       router,
		storage:      storage,
		pendingQueue: make(chan *QueuedPayment, 10000), // Large buffer for high throughput
		retryQueue:   make(chan *QueuedPayment, 1000),  // Smaller buffer for retries
		workers:      workers,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start begins processing payments
func (pq *PaymentQueue) Start() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.stopped {
		return
	}

	// Start main processing workers
	for i := 0; i < pq.workers; i++ {
		pq.workerWG.Add(1)
		go pq.processingWorker(i)
	}

	// Start retry worker
	pq.retryWorkerWG.Add(1)
	go pq.retryWorker()

	// Start retry scheduler
	pq.retryWorkerWG.Add(1)
	go pq.retryScheduler()

	// Recover any failed payments from storage on startup
	go pq.recoverFailedPayments()

	log.Printf("Payment queue started with %d workers", pq.workers)
}

// SubmitPayment adds a payment to the processing queue
func (pq *PaymentQueue) SubmitPayment(correlationID string, amount float64) error {
	payment := &QueuedPayment{
		CorrelationID: correlationID,
		Amount:        amount,
		RequestedAt:   time.Now().UTC(),
		Status:        StatusPending,
		MaxRetries:    5, // Configurable retry limit
		AttemptCount:  0,
	}

	// Persist to storage immediately
	if err := pq.storage.SavePayment(payment); err != nil {
		return fmt.Errorf("failed to persist payment: %w", err)
	}

	// Add to processing queue
	select {
	case pq.pendingQueue <- payment:
		return nil
	case <-pq.ctx.Done():
		return fmt.Errorf("payment queue is shutting down")
	default:
		// Queue is full - this is a back-pressure mechanism
		return fmt.Errorf("payment queue is full, try again later")
	}
}

// processingWorker processes payments from the main queue
func (pq *PaymentQueue) processingWorker(workerID int) {
	defer pq.workerWG.Done()

	log.Printf("Processing worker %d started", workerID)

	for {
		select {
		case payment := <-pq.pendingQueue:
			pq.processPayment(payment, workerID)
		case <-pq.ctx.Done():
			log.Printf("Processing worker %d stopping", workerID)
			return
		}
	}
}

// processPayment attempts to process a single payment
func (pq *PaymentQueue) processPayment(payment *QueuedPayment, workerID int) {
	// Update status to processing
	payment.Status = StatusProcessing
	payment.AttemptCount++
	payment.LastAttempt = time.Now()

	pq.storage.UpdatePaymentStatus(payment.CorrelationID, StatusProcessing, "")

	log.Printf("Worker %d processing payment %s (attempt %d/%d)",
		workerID, payment.CorrelationID, payment.AttemptCount, payment.MaxRetries)

	// Create context with timeout for this payment
	ctx, cancel := context.WithTimeout(pq.ctx, 30*time.Second)
	defer cancel()

	// Try to route the payment
	processorUsed, err := pq.router.RoutePayment(ctx, payment.CorrelationID, payment.Amount)

	if err == nil {
		// SUCCESS: Payment processed
		payment.Status = StatusProcessed
		payment.ProcessedBy = processorUsed

		pq.storage.UpdatePaymentStatus(payment.CorrelationID, StatusProcessed, processorUsed)

		log.Printf("Payment %s successfully processed by %s processor",
			payment.CorrelationID, processorUsed)
		return
	}

	// FAILURE: Handle the error
	payment.LastError = err.Error()
	payment.FailureCount++

	log.Printf("Payment %s failed (attempt %d/%d): %v",
		payment.CorrelationID, payment.AttemptCount, payment.MaxRetries, err)

	// Check if we should retry
	if payment.AttemptCount < payment.MaxRetries {
		// Schedule for retry with exponential backoff
		pq.scheduleRetry(payment)
	} else {
		// Max retries exceeded - mark as failed
		payment.Status = StatusFailed
		pq.storage.UpdatePaymentStatus(payment.CorrelationID, StatusFailed, "")

		log.Printf("Payment %s failed permanently after %d attempts",
			payment.CorrelationID, payment.AttemptCount)

		// Could trigger alert/notification here
		pq.handlePermanentFailure(payment)
	}
}

// scheduleRetry schedules a payment for retry with exponential backoff
func (pq *PaymentQueue) scheduleRetry(payment *QueuedPayment) {
	payment.Status = StatusRetrying

	// Exponential backoff: 2^attempt * base_delay
	// Attempt 1: 2s, Attempt 2: 4s, Attempt 3: 8s, etc.
	baseDelay := 2 * time.Second
	backoffDelay := time.Duration(1<<uint(payment.AttemptCount-1)) * baseDelay

	// Cap the maximum delay at 5 minutes
	if backoffDelay > 5*time.Minute {
		backoffDelay = 5 * time.Minute
	}

	payment.NextRetry = time.Now().Add(backoffDelay)

	pq.storage.UpdatePaymentStatus(payment.CorrelationID, StatusRetrying, "")

	log.Printf("Payment %s scheduled for retry in %v", payment.CorrelationID, backoffDelay)

	// Add to retry queue (non-blocking)
	select {
	case pq.retryQueue <- payment:
	default:
		log.Printf("Retry queue full, payment %s will be recovered on restart", payment.CorrelationID)
	}
}

// retryScheduler moves payments from retry queue back to main queue when ready
func (pq *PaymentQueue) retryScheduler() {
	defer pq.retryWorkerWG.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var retryList []*QueuedPayment

	for {
		select {
		case payment := <-pq.retryQueue:
			retryList = append(retryList, payment)

		case <-ticker.C:
			// Check if any payments are ready for retry
			now := time.Now()
			remaining := retryList[:0] // Reuse slice

			for _, payment := range retryList {
				if now.After(payment.NextRetry) {
					// Ready for retry - send back to main queue
					select {
					case pq.pendingQueue <- payment:
						log.Printf("Payment %s moved back to processing queue", payment.CorrelationID)
					default:
						// Main queue full, keep in retry list
						remaining = append(remaining, payment)
					}
				} else {
					// Not ready yet, keep in retry list
					remaining = append(remaining, payment)
				}
			}

			retryList = remaining

		case <-pq.ctx.Done():
			return
		}
	}
}

// retryWorker handles the retry scheduling
func (pq *PaymentQueue) retryWorker() {
	defer pq.retryWorkerWG.Done()
	// The retryScheduler handles the actual work
}

// recoverFailedPayments recovers any failed payments from storage on startup
func (pq *PaymentQueue) recoverFailedPayments() {
	log.Println("Recovering failed payments from storage...")

	failedPayments, err := pq.storage.GetFailedPayments()
	if err != nil {
		log.Printf("Failed to recover payments from storage: %v", err)
		return
	}

	recovered := 0
	for _, payment := range failedPayments {
		// Only recover payments that haven't exceeded max retries
		if payment.AttemptCount < payment.MaxRetries {
			select {
			case pq.pendingQueue <- payment:
				recovered++
			default:
				log.Printf("Queue full, cannot recover payment %s", payment.CorrelationID)
			}
		}
	}

	log.Printf("Recovered %d failed payments", recovered)
}

// handlePermanentFailure handles payments that have permanently failed
func (pq *PaymentQueue) handlePermanentFailure(payment *QueuedPayment) {
	// In a real system, you might:
	// 1. Send alert to operations team
	// 2. Create manual review task
	// 3. Trigger refund process
	// 4. Log to external monitoring system

	log.Printf("ALERT: Payment %s permanently failed - manual intervention required",
		payment.CorrelationID)

	// Could implement dead letter queue here
	// deadLetterQueue.Add(payment)
}

// Stop gracefully shuts down the payment queue
func (pq *PaymentQueue) Stop() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.stopped {
		return
	}

	pq.stopped = true
	pq.cancel()

	// Wait for workers to finish
	pq.workerWG.Wait()
	pq.retryWorkerWG.Wait()

	log.Println("Payment queue stopped")
}

// GetStats returns queue statistics
func (pq *PaymentQueue) GetStats() map[string]any {
	return map[string]any{
		"pendingQueueSize": len(pq.pendingQueue),
		"retryQueueSize":   len(pq.retryQueue),
		"workers":          pq.workers,
		"status":           "running",
	}
}
