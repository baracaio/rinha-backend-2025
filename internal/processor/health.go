package processor

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type (
	ProcessorHealth struct {
		Failing         bool
		MinResponseTime int
		LastChecked     time.Time
		CheckCount      int64
		FailureCount    int64
	}

	HealthMonitor struct {
		defaultClient  *Client
		fallbackClient *Client

		defaultHealthy  int32
		fallbackHealthy int32

		mu             sync.RWMutex
		defaultHealth  *ProcessorHealth
		fallbackHealth *ProcessorHealth

		stopChan chan struct{}
		stopped  int32
		wg       sync.WaitGroup
	}
)

func NewHealthMonitor(defaultClient, fallbackClient *Client) *HealthMonitor {
	return &HealthMonitor{
		defaultClient:  defaultClient,
		fallbackClient: fallbackClient,
		defaultHealth: &ProcessorHealth{
			Failing:         false,
			MinResponseTime: 100,
			LastChecked:     time.Now(),
		},
		fallbackHealth: &ProcessorHealth{
			Failing:         false,
			MinResponseTime: 100,
			LastChecked:     time.Now(),
		},
		stopChan: make(chan struct{}),
	}
}

func (hm *HealthMonitor) Start() {
	atomic.StoreInt32(&hm.defaultHealthy, 1)
	atomic.StoreInt32(&hm.fallbackHealthy, 1)

	hm.wg.Add(1)
	go hm.monitorHealth()

	log.Println("Health monitor started")
}

func (hm *HealthMonitor) Stop() {
	if atomic.CompareAndSwapInt32(&hm.stopped, 0, 1) {
		close(hm.stopChan)
		hm.wg.Wait()
		log.Println("Health monitor stopped")
	}
}

func (hm *HealthMonitor) IsDefaultHealthy() bool {
	return atomic.LoadInt32(&hm.defaultHealthy) == 1
}

func (hm *HealthMonitor) IsFallbackHealthy() bool {
	return atomic.LoadInt32(&hm.fallbackHealthy) == 1
}

func (hm *HealthMonitor) GetDefaultHealth() *ProcessorHealth {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	health := *hm.defaultHealth
	return &health
}

func (hm *HealthMonitor) GetFallbackHealth() *ProcessorHealth {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	health := *hm.fallbackHealth
	return &health
}

func (hm *HealthMonitor) monitorHealth() {
	defer hm.wg.Done()

	hm.checkProcessorHealth()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hm.checkProcessorHealth()
		case <-hm.stopChan:
			return
		}
	}
}

func (hm *HealthMonitor) checkProcessorHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		hm.checkSingleProcessor(ctx, hm.defaultClient, "default")
	}()

	go func() {
		defer wg.Done()
		hm.checkSingleProcessor(ctx, hm.fallbackClient, "fallback")
	}()

	wg.Wait()
}

func (hm *HealthMonitor) checkSingleProcessor(ctx context.Context, client *Client, processorType string) {
	startTime := time.Now()
	health, err := client.CheckHealth(ctx)
	checkDuration := time.Since(startTime)

	hm.mu.Lock()
	defer hm.mu.Unlock()

	var processorHealth *ProcessorHealth
	var healthyFlag *int32

	if processorType == "default" {
		processorHealth = hm.defaultHealth
		healthyFlag = &hm.defaultHealthy
	} else {
		processorHealth = hm.fallbackHealth
		healthyFlag = &hm.fallbackHealthy
	}

	processorHealth.LastChecked = time.Now()
	processorHealth.CheckCount++

	if err != nil {
		processorHealth.Failing = true
		processorHealth.FailureCount++
		atomic.StoreInt32(healthyFlag, 0)

		if processorHealth.CheckCount%10 == 0 {
			log.Printf("Health check failed for %s processor: %v (failures: %d/%d)",
				processorType, err, processorHealth.FailureCount, processorHealth.CheckCount)
		}
		return
	}

	processorHealth.Failing = health.Failing
	processorHealth.MinResponseTime = health.MinResponseTime

	isHealthy := !health.Failing && checkDuration < 5*time.Second

	if isHealthy {
		atomic.StoreInt32(healthyFlag, 1)
	} else {
		atomic.StoreInt32(healthyFlag, 0)
		processorHealth.FailureCount++
	}

	previousHealthy := atomic.LoadInt32(healthyFlag) == 1
	if previousHealthy != isHealthy {
		if isHealthy {
			log.Printf("%s processor is now healthy (response time: %v, min: %dms)",
				processorType, checkDuration, health.MinResponseTime)
		} else {
			log.Printf("%s processor is now unhealthy (failing: %t, response time: %v)",
				processorType, health.Failing, checkDuration)
		}
	}
}

func (hm *HealthMonitor) GetHealthStats() map[string]interface{} {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	defaultSuccessRate := float64(0)
	if hm.defaultHealth.CheckCount > 0 {
		defaultSuccessRate = float64(hm.defaultHealth.CheckCount-hm.defaultHealth.FailureCount) / float64(hm.defaultHealth.CheckCount) * 100
	}

	fallbackSuccessRate := float64(0)
	if hm.fallbackHealth.CheckCount > 0 {
		fallbackSuccessRate = float64(hm.fallbackHealth.CheckCount-hm.fallbackHealth.FailureCount) / float64(hm.fallbackHealth.CheckCount) * 100
	}

	return map[string]interface{}{
		"default": map[string]interface{}{
			"healthy":         hm.IsDefaultHealthy(),
			"failing":         hm.defaultHealth.Failing,
			"minResponseTime": hm.defaultHealth.MinResponseTime,
			"checkCount":      hm.defaultHealth.CheckCount,
			"failureCount":    hm.defaultHealth.FailureCount,
			"successRate":     defaultSuccessRate,
			"lastChecked":     hm.defaultHealth.LastChecked,
		},
		"fallback": map[string]interface{}{
			"healthy":         hm.IsFallbackHealthy(),
			"failing":         hm.fallbackHealth.Failing,
			"minResponseTime": hm.fallbackHealth.MinResponseTime,
			"checkCount":      hm.fallbackHealth.CheckCount,
			"failureCount":    hm.fallbackHealth.FailureCount,
			"successRate":     fallbackSuccessRate,
			"lastChecked":     hm.fallbackHealth.LastChecked,
		},
	}
}
