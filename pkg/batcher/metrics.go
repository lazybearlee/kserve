package batcher

import (
	"sync"
	"time"
)

type BatchMetrics struct {
	BatchSize        int
	QueueLength      int
	ProcessingTimeMs float64
	BaseLatencyMs    float64 // 新增L0记录
	ArrivalRate      float64 // 新增到达率记录
	Timestamp        time.Time
}

type MetricsCollector struct {
	windowSize  int
	metricsRing []BatchMetrics
	pointer     int
	filled      bool
	mutex       sync.RWMutex
}

func NewMetricsCollector(windowSize int) *MetricsCollector {
	return &MetricsCollector{
		windowSize:  windowSize,
		metricsRing: make([]BatchMetrics, windowSize),
	}
}

func (mc *MetricsCollector) AddMetric(metric BatchMetrics) {
	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.metricsRing[mc.pointer] = metric
	mc.pointer = (mc.pointer + 1) % mc.windowSize
	if !mc.filled && mc.pointer == 0 {
		mc.filled = true
	}
}

func (mc *MetricsCollector) GetRecentMetrics(n int) []BatchMetrics {
	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	if n <= 0 || n > mc.windowSize {
		n = mc.windowSize
	}
	if !mc.filled && n > mc.pointer {
		n = mc.pointer
	}

	result := make([]BatchMetrics, n)
	start := (mc.pointer - n + mc.windowSize) % mc.windowSize
	for i := 0; i < n; i++ {
		pos := (start + i) % mc.windowSize
		result[i] = mc.metricsRing[pos]
	}
	return result
}
