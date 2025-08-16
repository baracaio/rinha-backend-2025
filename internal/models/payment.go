package models

import (
	"time"
)

type (
	PaymentStatus string
	ProcessorType string

	Payment struct {
		CorrelationID string        `json:"correlationId"`
		Amount        float64       `json:"amount"`
		RequestedAt   time.Time     `json:"requestedAt"`
		ProcessedBy   ProcessorType `json:"processedBy,omitempty"`
		Status        PaymentStatus `json:"status"`
		ProcessedAt   *time.Time    `json:"processedAt,omitempty"`
		ErrorMessage  string        `json:"errorMessage,omitempty"`
	}

	PaymentRequest struct {
		CorrelationID string  `json:"correlationId" validate:"required,uuid"`
		Amount        float64 `json:"amount" validate:"required,gt=0"`
	}

	PaymentProcessorRequest struct {
		CorrelationID string    `json:"correlationId"`
		Amount        float64   `json:"amount"`
		RequestedAt   time.Time `json:"requestedAt"`
	}
)

const (
	PaymentStatusPending   PaymentStatus = "pending"
	PaymentStatusProcessed PaymentStatus = "processed"
	PaymentStatusFailed    PaymentStatus = "failed"

	ProcessorTypeDefault  ProcessorType = "default"
	ProcessorTypeFallback ProcessorType = "fallback"
)

func NewPayment(correlationID string, amount float64) *Payment {
	return &Payment{
		CorrelationID: correlationID,
		Amount:        amount,
		RequestedAt:   time.Now().UTC(),
		Status:        PaymentStatusPending,
	}
}

func (p *Payment) MarkAsProcessed(processor ProcessorType) {
	now := time.Now().UTC()
	p.Status = PaymentStatusProcessed
	p.ProcessedBy = processor
	p.ProcessedAt = &now
	p.ErrorMessage = ""
}

func (p *Payment) MarkAsFailed(errorMsg string) {
	now := time.Now().UTC()
	p.Status = PaymentStatusFailed
	p.ProcessedAt = &now
	p.ErrorMessage = errorMsg
}

func (p *Payment) IsProcessed() bool {
	return p.Status == PaymentStatusProcessed || p.Status == PaymentStatusFailed
}

func (p *Payment) ToProcessorRequest() *PaymentProcessorRequest {
	return &PaymentProcessorRequest{
		CorrelationID: p.CorrelationID,
		Amount:        p.Amount,
		RequestedAt:   p.RequestedAt,
	}
}
