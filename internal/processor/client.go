package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type (
	// PaymentRequest represents the request payload for payment processing
	PaymentRequest struct {
		CorrelationID string    `json:"correlationId"`
		Amount        float64   `json:"amount"`
		RequestedAt   time.Time `json:"requestedAt"`
	}

	// PaymentResponse represents the response from payment processors
	PaymentResponse struct {
		Message string `json:"message"`
	}

	// HealthResponse represents the health check response
	HealthResponse struct {
		Failing         bool `json:"failing"`
		MinResponseTime int  `json:"minResponseTime"`
	}

	// Client represents a payment processor client
	Client struct {
		baseURL    string
		httpClient *http.Client
		mu         sync.RWMutex
		lastHealth *HealthResponse
		lastCheck  time.Time
	}
)

// NewClient creates a new payment processor client with optimized HTTP settings
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   10,
				IdleConnTimeout:       90 * time.Second,
				DisableKeepAlives:     false,
				ResponseHeaderTimeout: 5 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
			Timeout: 10 * time.Second,
		},
	}
}

// ProcessPayment sends a payment request to the processor
func (c *Client) ProcessPayment(ctx context.Context, correlationID string, amount float64) error {
	// Prepare the payment request
	paymentReq := PaymentRequest{
		CorrelationID: correlationID,
		Amount:        amount,
		RequestedAt:   time.Now().UTC(),
	}

	// Marshal the request body
	body, err := json.Marshal(paymentReq)
	if err != nil {
		return fmt.Errorf("failed to marshal payment request: %w", err)
	}

	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/payments", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("payment processing failed with status: %d", resp.StatusCode)
	}

	// Always read and close the response body to reuse connections
	var paymentResp PaymentResponse
	if err := json.NewDecoder(resp.Body).Decode(&paymentResp); err != nil {
		// Don't fail on decode errors for payment processing
		// Some processors might return empty bodies or different formats
		// The important thing is that we got a 2xx status code
		log.Printf("Warning: failed to decode payment response (status %d): %v", resp.StatusCode, err)
	}

	return nil
}

// CheckHealth performs a health check on the payment processor
// Note: This is rate-limited to 1 call per 5 seconds as per the API spec
func (c *Client) CheckHealth(ctx context.Context) (*HealthResponse, error) {
	c.mu.RLock()
	// Return cached result if we checked within the last 5 seconds
	if time.Since(c.lastCheck) < 5*time.Second && c.lastHealth != nil {
		health := *c.lastHealth
		c.mu.RUnlock()
		return &health, nil
	}
	c.mu.RUnlock()

	// Acquire write lock for actual health check
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock (avoid race condition)
	if time.Since(c.lastCheck) < 5*time.Second && c.lastHealth != nil {
		health := *c.lastHealth
		return &health, nil
	}

	// Create the HTTP request for health check
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/payments/service-health", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create health check request: %w", err)
	}

	// Send the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// If health check fails, assume the service is unhealthy
		unhealthyResponse := &HealthResponse{
			Failing:         true,
			MinResponseTime: 0,
		}
		c.lastHealth = unhealthyResponse
		c.lastCheck = time.Now()
		return unhealthyResponse, fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if resp.StatusCode != http.StatusOK {
		unhealthyResponse := &HealthResponse{
			Failing:         true,
			MinResponseTime: 0,
		}
		c.lastHealth = unhealthyResponse
		c.lastCheck = time.Now()
		return unhealthyResponse, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	// Decode the health response
	var healthResp HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&healthResp); err != nil {
		unhealthyResponse := &HealthResponse{
			Failing:         true,
			MinResponseTime: 0,
		}
		c.lastHealth = unhealthyResponse
		c.lastCheck = time.Now()
		return unhealthyResponse, fmt.Errorf("failed to decode health response: %w", err)
	}

	// Cache the result
	c.lastHealth = &healthResp
	c.lastCheck = time.Now()

	return &healthResp, nil
}

// GetCachedHealth returns the last cached health status without making a new request
func (c *Client) GetCachedHealth() *HealthResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lastHealth == nil {
		// Return a default "unknown" state if no health check has been performed
		return &HealthResponse{
			Failing:         false, // Assume healthy until proven otherwise
			MinResponseTime: 100,   // Default response time
		}
	}

	// Return a copy to avoid race conditions
	health := *c.lastHealth
	return &health
}

// IsHealthy returns true if the processor is considered healthy
func (c *Client) IsHealthy() bool {
	health := c.GetCachedHealth()
	return !health.Failing
}

// GetBaseURL returns the base URL of the processor
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// Close closes the client and cleans up resources
func (c *Client) Close() {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
}
