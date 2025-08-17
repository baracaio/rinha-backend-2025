package storage

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type (
	// PaymentStatus represents the current status of a payment
	PaymentStatus string

	// QueuedPayment represents a payment in the system
	QueuedPayment struct {
		CorrelationID string        `json:"correlationId"`
		Amount        float64       `json:"amount"`
		RequestedAt   time.Time     `json:"requestedAt"`
		Status        PaymentStatus `json:"status"`
		ProcessedBy   string        `json:"processedBy,omitempty"`
		ProcessedAt   *time.Time    `json:"processedAt,omitempty"`

		// Retry tracking
		AttemptCount int       `json:"attemptCount"`
		LastAttempt  time.Time `json:"lastAttempt"`
		NextRetry    time.Time `json:"nextRetry"`
		MaxRetries   int       `json:"maxRetries"`

		// Error tracking
		LastError    string `json:"lastError,omitempty"`
		FailureCount int    `json:"failureCount"`

		// Timestamps
		CreatedAt time.Time `json:"createdAt"`
		UpdatedAt time.Time `json:"updatedAt"`
	}

	// SQLiteStorage implements PaymentStorage using SQLite
	SQLiteStorage struct {
		db     *sql.DB
		dbPath string
		mu     sync.RWMutex
	}
)

const (
	StatusPending    PaymentStatus = "pending"
	StatusProcessing PaymentStatus = "processing"
	StatusProcessed  PaymentStatus = "processed"
	StatusFailed     PaymentStatus = "failed"
	StatusRetrying   PaymentStatus = "retrying"
)

// NewSQLiteStorage creates a new SQLite storage instance
func NewSQLiteStorage(dbPath string) (*SQLiteStorage, error) {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Open database with optimized settings for performance
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_synchronous=NORMAL&_cache_size=10000&_busy_timeout=30000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool for high concurrency
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	storage := &SQLiteStorage{
		db:     db,
		dbPath: dbPath,
	}

	if err := storage.initialize(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	log.Printf("SQLite storage initialized at %s", dbPath)
	return storage, nil
}

// initialize creates tables and indexes
func (s *SQLiteStorage) initialize() error {
	// Create payments table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS payments (
		correlation_id TEXT PRIMARY KEY,
		amount REAL NOT NULL,
		requested_at DATETIME NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		processed_by TEXT,
		processed_at DATETIME,
		
		-- Retry tracking
		attempt_count INTEGER DEFAULT 0,
		last_attempt DATETIME,
		next_retry DATETIME,
		max_retries INTEGER DEFAULT 5,
		
		-- Error tracking
		last_error TEXT,
		failure_count INTEGER DEFAULT 0,
		
		-- Timestamps
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	if _, err := s.db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create payments table: %w", err)
	}

	// Create indexes for performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);",
		"CREATE INDEX IF NOT EXISTS idx_payments_requested_at ON payments(requested_at);",
		"CREATE INDEX IF NOT EXISTS idx_payments_processed_by ON payments(processed_by);",
		"CREATE INDEX IF NOT EXISTS idx_payments_next_retry ON payments(next_retry);",
		"CREATE INDEX IF NOT EXISTS idx_payments_status_retry ON payments(status, next_retry);",
	}

	for _, indexSQL := range indexes {
		if _, err := s.db.Exec(indexSQL); err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}

// SavePayment saves a new payment to the database
func (s *SQLiteStorage) SavePayment(payment *QueuedPayment) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	if payment.CreatedAt.IsZero() {
		payment.CreatedAt = now
	}
	payment.UpdatedAt = now

	query := `
	INSERT INTO payments (
		correlation_id, amount, requested_at, status, processed_by, processed_at,
		attempt_count, last_attempt, next_retry, max_retries,
		last_error, failure_count, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.Exec(query,
		payment.CorrelationID,
		payment.Amount,
		payment.RequestedAt,
		payment.Status,
		nullableString(payment.ProcessedBy),
		nullableTime(payment.ProcessedAt),
		payment.AttemptCount,
		nullableTime(&payment.LastAttempt),
		nullableTime(&payment.NextRetry),
		payment.MaxRetries,
		nullableString(payment.LastError),
		payment.FailureCount,
		payment.CreatedAt,
		payment.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to save payment %s: %w", payment.CorrelationID, err)
	}

	return nil
}

// UpdatePaymentStatus updates the status of a payment
func (s *SQLiteStorage) UpdatePaymentStatus(correlationID string, status PaymentStatus, processedBy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	var processedAt *time.Time

	if status == StatusProcessed {
		processedAt = &now
	}

	query := `
	UPDATE payments 
	SET status = ?, processed_by = ?, processed_at = ?, updated_at = ?
	WHERE correlation_id = ?`

	result, err := s.db.Exec(query, status, nullableString(processedBy), nullableTime(processedAt), now, correlationID)
	if err != nil {
		return fmt.Errorf("failed to update payment %s: %w", correlationID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("payment %s not found", correlationID)
	}

	return nil
}

// GetPayment retrieves a payment by correlation ID
func (s *SQLiteStorage) GetPayment(correlationID string) (*QueuedPayment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
	SELECT correlation_id, amount, requested_at, status, processed_by, processed_at,
		   attempt_count, last_attempt, next_retry, max_retries,
		   last_error, failure_count, created_at, updated_at
	FROM payments WHERE correlation_id = ?`

	row := s.db.QueryRow(query, correlationID)

	payment := &QueuedPayment{}
	var processedBy, lastError sql.NullString
	var processedAt, lastAttempt, nextRetry sql.NullTime

	err := row.Scan(
		&payment.CorrelationID,
		&payment.Amount,
		&payment.RequestedAt,
		&payment.Status,
		&processedBy,
		&processedAt,
		&payment.AttemptCount,
		&lastAttempt,
		&nextRetry,
		&payment.MaxRetries,
		&lastError,
		&payment.FailureCount,
		&payment.CreatedAt,
		&payment.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("payment %s not found", correlationID)
		}
		return nil, fmt.Errorf("failed to get payment %s: %w", correlationID, err)
	}

	// Handle nullable fields
	if processedBy.Valid {
		payment.ProcessedBy = processedBy.String
	}
	if processedAt.Valid {
		payment.ProcessedAt = &processedAt.Time
	}
	if lastAttempt.Valid {
		payment.LastAttempt = lastAttempt.Time
	}
	if nextRetry.Valid {
		payment.NextRetry = nextRetry.Time
	}
	if lastError.Valid {
		payment.LastError = lastError.String
	}

	return payment, nil
}

// GetFailedPayments retrieves all payments that can be retried
func (s *SQLiteStorage) GetFailedPayments() ([]*QueuedPayment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
	SELECT correlation_id, amount, requested_at, status, processed_by, processed_at,
		   attempt_count, last_attempt, next_retry, max_retries,
		   last_error, failure_count, created_at, updated_at
	FROM payments 
	WHERE status IN ('failed', 'retrying', 'pending') 
	AND attempt_count < max_retries
	ORDER BY requested_at ASC`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get failed payments: %w", err)
	}
	defer rows.Close()

	var payments []*QueuedPayment

	for rows.Next() {
		payment := &QueuedPayment{}
		var processedBy, lastError sql.NullString
		var processedAt, lastAttempt, nextRetry sql.NullTime

		err := rows.Scan(
			&payment.CorrelationID,
			&payment.Amount,
			&payment.RequestedAt,
			&payment.Status,
			&processedBy,
			&processedAt,
			&payment.AttemptCount,
			&lastAttempt,
			&nextRetry,
			&payment.MaxRetries,
			&lastError,
			&payment.FailureCount,
			&payment.CreatedAt,
			&payment.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan payment: %w", err)
		}

		// Handle nullable fields
		if processedBy.Valid {
			payment.ProcessedBy = processedBy.String
		}
		if processedAt.Valid {
			payment.ProcessedAt = &processedAt.Time
		}
		if lastAttempt.Valid {
			payment.LastAttempt = lastAttempt.Time
		}
		if nextRetry.Valid {
			payment.NextRetry = nextRetry.Time
		}
		if lastError.Valid {
			payment.LastError = lastError.String
		}

		payments = append(payments, payment)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating failed payments: %w", err)
	}

	return payments, nil
}

// GetPaymentsSummary returns aggregated payment data for the specified time range
func (s *SQLiteStorage) GetPaymentsSummary(from, to time.Time) (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
	SELECT 
		processed_by,
		COUNT(*) as total_requests,
		COALESCE(SUM(amount), 0) as total_amount
	FROM payments 
	WHERE status = 'processed' 
	AND processed_at >= ? 
	AND processed_at <= ?
	AND processed_by IS NOT NULL
	GROUP BY processed_by`

	rows, err := s.db.Query(query, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to get payments summary: %w", err)
	}
	defer rows.Close()

	summary := map[string]interface{}{
		"default": map[string]interface{}{
			"totalRequests": int64(0),
			"totalAmount":   float64(0),
		},
		"fallback": map[string]interface{}{
			"totalRequests": int64(0),
			"totalAmount":   float64(0),
		},
	}

	for rows.Next() {
		var processorType string
		var totalRequests int64
		var totalAmount float64

		err := rows.Scan(&processorType, &totalRequests, &totalAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan summary row: %w", err)
		}

		if processorType == "default" || processorType == "fallback" {
			summary[processorType] = map[string]interface{}{
				"totalRequests": totalRequests,
				"totalAmount":   totalAmount,
			}
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating summary rows: %w", err)
	}

	return summary, nil
}

// UpdatePaymentRetry updates retry-related fields for a payment
func (s *SQLiteStorage) UpdatePaymentRetry(correlationID string, attemptCount, failureCount int, lastAttempt, nextRetry time.Time, lastError string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `
	UPDATE payments 
	SET attempt_count = ?, failure_count = ?, last_attempt = ?, next_retry = ?, last_error = ?, updated_at = ?
	WHERE correlation_id = ?`

	_, err := s.db.Exec(query, attemptCount, failureCount, lastAttempt, nextRetry, lastError, time.Now().UTC(), correlationID)
	if err != nil {
		return fmt.Errorf("failed to update payment retry info %s: %w", correlationID, err)
	}

	return nil
}

// GetStats returns storage statistics
func (s *SQLiteStorage) GetStats() (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query := `
	SELECT 
		status,
		COUNT(*) as count,
		COALESCE(AVG(amount), 0) as avg_amount,
		COALESCE(SUM(amount), 0) as total_amount
	FROM payments 
	GROUP BY status`

	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]interface{})
	totalPayments := int64(0)

	for rows.Next() {
		var status string
		var count int64
		var avgAmount, totalAmount float64

		err := rows.Scan(&status, &count, &avgAmount, &totalAmount)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stats row: %w", err)
		}

		stats[status] = map[string]interface{}{
			"count":       count,
			"avgAmount":   avgAmount,
			"totalAmount": totalAmount,
		}

		totalPayments += count
	}

	stats["total"] = totalPayments

	return stats, nil
}

// Close closes the database connection
func (s *SQLiteStorage) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Helper functions
func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullableTime(t *time.Time) interface{} {
	if t == nil || t.IsZero() {
		return nil
	}
	return t
}
