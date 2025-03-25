// optimizer.go
package batcher

import (
	"math"
	"time"
)

const (
	lambdaPenalty = 1000
	alpha         = 1
	beta          = 1
)

type BatchOptimizer struct {
	estimator     *ParameterEstimator
	handler       *BatchHandler
	adjustTicker  *time.Ticker
	lambdaPenalty float64
	alpha         float64
	beta          float64
}

func NewBatchOptimizer(estimator *ParameterEstimator, handler *BatchHandler, interval time.Duration) *BatchOptimizer {
	return &BatchOptimizer{
		estimator:    estimator,
		handler:      handler,
		adjustTicker: time.NewTicker(interval),
	}
}

func (bo *BatchOptimizer) Run() {
	defer bo.adjustTicker.Stop()

	for range bo.adjustTicker.C {
		bo.adjustBatchSize()
	}
}

func (bo *BatchOptimizer) adjustBatchSize() {
	// 获取当前参数
	gamma1, gamma2, delta := bo.estimator.GetCurrentParameters()

	// 获取当前系统状态
	Q := bo.handler.getCurrentQueueLength()
	r, err := getArrivalRate()
	if err != nil {
		bo.handler.log.Errorf("Failed to get arrival rate: %v", err)
		return
	}
	L0, err := getBaseLatency()
	if err != nil {
		bo.handler.log.Errorf("Failed to get base latency: %v", err)
		return
	}

	// 优化批处理大小
	bestB := bo.findOptimalBatchSize(Q, r, gamma1, gamma2, delta, L0)

	// 应用新的大小
	bo.handler.setDynamicBatchSize(bestB)

	// 记录日志
	bo.handler.log.Infof("Batch size adjusted - Q: %.2f, r: %.2f, gamma1: %.3f, gamma2: %.3f, delta: %.3f, bestB: %d",
		Q, r, gamma1, gamma2, delta, bestB)
}

func (bo *BatchOptimizer) findOptimalBatchSize(Q, r, gamma1, gamma2, delta, L0 float64) int {
	bestB := 1
	bestF := bo.calculateObjective(1, Q, r, gamma1, gamma2, delta, L0)

	for B := 2; B <= bo.handler.MaxBatchSize; B++ {
		currentF := bo.calculateObjective(B, Q, r, gamma1, gamma2, delta, L0)
		if currentF < bestF {
			bestF = currentF
			bestB = B
		}
	}

	return bestB
}

// F(B) = alpha * L + penalty - beta * throughput
func (bo *BatchOptimizer) calculateObjective(B int, Q, r, gamma1, gamma2, delta, L0 float64) float64 {
	L := L0 + gamma1*float64(B) + gamma2*Q
	penalty := lambdaPenalty * math.Max(0, L-float64(bo.handler.MaxLatency))
	throughput := r * (1 - math.Exp(-delta*float64(B)))
	return alpha*L + penalty - beta*throughput
}
