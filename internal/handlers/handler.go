package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// PaymentRequest represents the incoming payment request
type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}

// PaymentResponse represents the payment response (if needed)
type PaymentResponse struct {
	Message string `json:"message,omitempty"`
}

// SummaryResponse represents the payment summary response
type SummaryResponse struct {
	Default struct {
		TotalRequests int64   `json:"totalRequests"`
		TotalAmount   float64 `json:"totalAmount"`
	} `json:"default"`
	Fallback struct {
		TotalRequests int64   `json:"totalRequests"`
		TotalAmount   float64 `json:"totalAmount"`
	} `json:"fallback"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// PaymentQueue interface for submitting payments
type PaymentQueue interface {
	SubmitPayment(correlationID string, amount float64) error
	GetStats() map[string]interface{}
}

// PaymentStorage interface for accessing payment data
type PaymentStorage interface {
	SavePayment(payment interface{}) error
	GetPayment(correlationID string) (interface{}, error)
	GetPaymentsSummary(from, to time.Time) (map[string]interface{}, error)
}

// PaymentHandler handles payment processing requests
type PaymentHandler struct {
	queue   PaymentQueue
	storage PaymentStorage
}

// NewPaymentHandler creates a new payment handler
func NewPaymentHandler(queue PaymentQueue, storage PaymentStorage) *PaymentHandler {
	return &PaymentHandler{
		queue:   queue,
		storage: storage,
	}
}

// HandlePayments handles POST /payments requests
func (h *PaymentHandler) HandlePayments(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method
	if r.Method != http.MethodPost {
		h.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/json")

	// Parse request body
	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Invalid JSON in payment request: %v", err)
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_JSON", "Request body must be valid JSON")
		return
	}

	// Validate correlation ID format (must be valid UUID)
	if _, err := uuid.Parse(req.CorrelationID); err != nil {
		log.Printf("Invalid correlation ID format: %s", req.CorrelationID)
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_CORRELATION_ID", "Correlation ID must be a valid UUID")
		return
	}

	// Validate amount (must be positive)
	if req.Amount <= 0 {
		log.Printf("Invalid amount: %f", req.Amount)
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_AMOUNT", "Amount must be positive")
		return
	}

	// Check for duplicate correlation ID
	if existingPayment, err := h.storage.GetPayment(req.CorrelationID); err == nil && existingPayment != nil {
		log.Printf("Duplicate correlation ID: %s", req.CorrelationID)
		h.writeErrorResponse(w, http.StatusConflict, "DUPLICATE_CORRELATION_ID", "Payment with this correlation ID already exists")
		return
	}

	// Submit payment to queue for async processing
	if err := h.queue.SubmitPayment(req.CorrelationID, req.Amount); err != nil {
		log.Printf("Failed to submit payment %s: %v", req.CorrelationID, err)

		// Check if it's a queue full error (back-pressure)
		if err.Error() == "payment queue is full, try again later" {
			h.writeErrorResponse(w, http.StatusServiceUnavailable, "QUEUE_FULL", "Payment queue is full, please try again later")
			return
		}

		// Other errors are internal server errors
		h.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to process payment")
		return
	}

	// Payment accepted for processing
	log.Printf("Payment %s accepted for processing (amount: %.2f)", req.CorrelationID, req.Amount)

	// Return 202 Accepted (any 2XX is valid according to spec)
	w.WriteHeader(http.StatusAccepted)

	// Optionally return a response body (not required by spec)
	response := PaymentResponse{
		Message: "Payment accepted for processing",
	}
	json.NewEncoder(w).Encode(response)
}

// SummaryHandler handles payment summary requests
type SummaryHandler struct {
	storage PaymentStorage
}

// NewSummaryHandler creates a new summary handler
func NewSummaryHandler(storage PaymentStorage) *SummaryHandler {
	return &SummaryHandler{
		storage: storage,
	}
}

// HandlePaymentsSummary handles GET /payments-summary requests
func (h *SummaryHandler) HandlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	// Only allow GET method
	if r.Method != http.MethodGet {
		h.writeErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET method is allowed")
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", "application/json")

	// Parse query parameters
	query := r.URL.Query()
	fromStr := query.Get("from")
	toStr := query.Get("to")

	// Validate required parameters
	if fromStr == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, "MISSING_FROM", "Missing required 'from' parameter")
		return
	}
	if toStr == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, "MISSING_TO", "Missing required 'to' parameter")
		return
	}

	// Parse timestamps (RFC3339 format)
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		log.Printf("Invalid 'from' timestamp: %s, error: %v", fromStr, err)
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_FROM", "Invalid 'from' timestamp format, expected RFC3339")
		return
	}

	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		log.Printf("Invalid 'to' timestamp: %s, error: %v", toStr, err)
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_TO", "Invalid 'to' timestamp format, expected RFC3339")
		return
	}

	// Validate time range
	if to.Before(from) {
		h.writeErrorResponse(w, http.StatusBadRequest, "INVALID_TIME_RANGE", "'to' timestamp must be after 'from' timestamp")
		return
	}

	// Prevent excessively large time ranges (optional optimization)
	if to.Sub(from) > 24*time.Hour*365 { // 1 year max
		h.writeErrorResponse(w, http.StatusBadRequest, "TIME_RANGE_TOO_LARGE", "Time range cannot exceed 1 year")
		return
	}

	// Get summary from storage
	summaryData, err := h.storage.GetPaymentsSummary(from, to)
	if err != nil {
		log.Printf("Failed to get payments summary: %v", err)
		h.writeErrorResponse(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to retrieve payment summary")
		return
	}

	// Convert to response format
	response := h.convertToSummaryResponse(summaryData)

	// Return 200 OK with summary data
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode summary response: %v", err)
	}

	log.Printf("Payment summary returned for period %s to %s", fromStr, toStr)
}

// HealthHandler provides a health check endpoint
type HealthHandler struct {
	queue   PaymentQueue
	storage PaymentStorage
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(queue PaymentQueue, storage PaymentStorage) *HealthHandler {
	return &HealthHandler{
		queue:   queue,
		storage: storage,
	}
}

// HandleHealth handles GET /health requests
func (h *HealthHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Get system stats
	queueStats := h.queue.GetStats()

	healthResponse := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"queue":     queueStats,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(healthResponse)
}

// Helper methods

// writeErrorResponse writes a standardized error response
func (h *PaymentHandler) writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error:   errorCode,
		Message: message,
	}

	json.NewEncoder(w).Encode(response)
}

// writeErrorResponse for SummaryHandler
func (h *SummaryHandler) writeErrorResponse(w http.ResponseWriter, statusCode int, errorCode, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error:   errorCode,
		Message: message,
	}

	json.NewEncoder(w).Encode(response)
}

// convertToSummaryResponse converts storage summary data to API response format
func (h *SummaryHandler) convertToSummaryResponse(summaryData map[string]interface{}) SummaryResponse {
	var response SummaryResponse

	// Extract default processor data
	if defaultData, ok := summaryData["default"].(map[string]interface{}); ok {
		if totalRequests, ok := defaultData["totalRequests"].(int64); ok {
			response.Default.TotalRequests = totalRequests
		}
		if totalAmount, ok := defaultData["totalAmount"].(float64); ok {
			response.Default.TotalAmount = totalAmount
		}
	}

	// Extract fallback processor data
	if fallbackData, ok := summaryData["fallback"].(map[string]interface{}); ok {
		if totalRequests, ok := fallbackData["totalRequests"].(int64); ok {
			response.Fallback.TotalRequests = totalRequests
		}
		if totalAmount, ok := fallbackData["totalAmount"].(float64); ok {
			response.Fallback.TotalAmount = totalAmount
		}
	}

	return response
}

// SetupRoutes sets up all the HTTP routes
func SetupRoutes(queue PaymentQueue, storage PaymentStorage) *http.ServeMux {
	mux := http.NewServeMux()

	// Create handlers
	paymentHandler := NewPaymentHandler(queue, storage)
	summaryHandler := NewSummaryHandler(storage)
	healthHandler := NewHealthHandler(queue, storage)

	// Register routes
	mux.HandleFunc("/payments", paymentHandler.HandlePayments)
	mux.HandleFunc("/payments-summary", summaryHandler.HandlePaymentsSummary)
	mux.HandleFunc("/health", healthHandler.HandleHealth)

	return mux
}

// Middleware for logging requests
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap the ResponseWriter to capture the status code
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Call the next handler
		next.ServeHTTP(wrappedWriter, r)

		// Log the request
		duration := time.Since(start)
		log.Printf("%s %s %d %v", r.Method, r.URL.Path, wrappedWriter.statusCode, duration)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Middleware for CORS (if needed)
func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Middleware for recovery from panics
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("Panic recovered: %v", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)

				response := ErrorResponse{
					Error:   "INTERNAL_ERROR",
					Message: "Internal server error",
				}
				json.NewEncoder(w).Encode(response)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
