package models

import (
	"fmt"
	"time"
)

type (
	ProcessorSummary struct {
		TotalRequests int     `json:"totalRequests"`
		TotalAmount   float64 `json:"totalAmount"`
	}

	PaymentSummary struct {
		Default  ProcessorSummary `json:"default"`
		Fallback ProcessorSummary `json:"fallback"`
	}

	SummaryRequest struct {
		From time.Time `json:"from" validate:"required"`
		To   time.Time `json:"to" validate:"required"`
	}

	SummaryFilter struct {
		From time.Time
		To   time.Time
	}
)

func NewSummaryRequest(from, to string) (*SummaryRequest, error) {
	f, err := time.Parse(time.RFC3339, from)
	if err != nil {
		return nil, fmt.Errorf("invalid 'from' parameter: %w", err)
	}

	t, err := time.Parse(time.RFC3339, to)
	if err != nil {
		return nil, fmt.Errorf("invalid 'to' parameter: %w", err)
	}

	if t.Before(f) {
		return nil, fmt.Errorf("'to' parameter must be after 'from' parameter")
	}

	return &SummaryRequest{
		f,
		t,
	}, nil
}

func NewPaymentSummary() *PaymentSummary {
	return &PaymentSummary{
		Default: ProcessorSummary{
			TotalRequests: 0,
			TotalAmount:   0.0,
		},
		Fallback: ProcessorSummary{
			TotalRequests: 0,
			TotalAmount:   0.0,
		},
	}
}

func (ps *PaymentSummary) AddPayment(payment *Payment) error {
	if payment.Status != PaymentStatusProcessed {
		return nil
	}

	switch payment.ProcessedBy {
	case ProcessorTypeDefault:
		ps.Default.TotalRequests++
		ps.Default.TotalAmount += payment.Amount
	case ProcessorTypeFallback:
		ps.Fallback.TotalRequests++
		ps.Fallback.TotalAmount += payment.Amount
	default:
		return fmt.Errorf("unknown processor type: %s", payment.ProcessedBy)
	}

	return nil
}

func (ps *PaymentSummary) AddDefaultPayment(amount float64) {
	ps.Default.TotalRequests++
	ps.Default.TotalAmount += amount
}

func (ps *PaymentSummary) AddFallbackPayment(amount float64) {
	ps.Fallback.TotalRequests++
	ps.Fallback.TotalAmount += amount
}

func (ps *PaymentSummary) Merge(other *PaymentSummary) {
	ps.Default.TotalRequests += other.Default.TotalRequests
	ps.Default.TotalAmount += other.Default.TotalAmount
	ps.Fallback.TotalRequests += other.Fallback.TotalRequests
	ps.Fallback.TotalAmount += other.Fallback.TotalAmount
}

func (ps *PaymentSummary) GetTotalRequests() int {
	return ps.Default.TotalRequests + ps.Fallback.TotalRequests
}

func (ps *PaymentSummary) GetTotalAmount() float64 {
	return ps.Default.TotalAmount + ps.Fallback.TotalAmount
}

func (ps *PaymentSummary) IsEmpty() bool {
	return ps.Default.TotalRequests == 0 && ps.Fallback.TotalRequests == 0
}

func PaymentInTimeRange(payment *Payment, filter *SummaryFilter) bool {
	checkTime := payment.RequestedAt
	if payment.ProcessedAt != nil {
		checkTime = *payment.ProcessedAt
	}

	return (checkTime.Equal(filter.From) || checkTime.After(filter.From)) &&
		(checkTime.Equal(filter.To) || checkTime.Before(filter.To))
}

func (ps *PaymentSummary) GetStats(filter *SummaryFilter) *SummaryStats {
	totalRequests := ps.GetTotalRequests()
	totalAmount := ps.GetTotalAmount()

	stats := &SummaryStats{
		PaymentSummary: *ps,
	}

	if totalRequests > 0 {
		stats.DefaultPercentage = float64(ps.Default.TotalRequests) / float64(totalRequests) * 100
		stats.FallbackPercentage = float64(ps.Fallback.TotalRequests) / float64(totalRequests) * 100
		stats.AverageAmount = totalAmount / float64(totalRequests)
	}

	if filter != nil {
		stats.TimeRange = fmt.Sprintf("%s to %s",
			filter.From.Format(time.RFC3339),
			filter.To.Format(time.RFC3339))
	}

	return stats
}

func (ps *PaymentSummary) Validate() error {
	if ps.Default.TotalRequests < 0 || ps.Fallback.TotalRequests < 0 {
		return fmt.Errorf("total requests cannot be negative")
	}

	if ps.Default.TotalAmount < 0 || ps.Fallback.TotalAmount < 0 {
		return fmt.Errorf("total amount cannot be negative")
	}

	return nil
}
