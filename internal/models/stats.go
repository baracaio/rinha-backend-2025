package models

type (
	SummaryStats struct {
		PaymentSummary
		DefaultPercentage  float64 `json:"defaultPercentage"`
		FallbackPercentage float64 `json:"fallbackPercentage"`
		AverageAmount      float64 `json:"averageAmount"`
		TimeRange          string  `json:"timeRange"`
	}
)
