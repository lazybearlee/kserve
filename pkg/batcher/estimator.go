// estimator.go
package batcher

import (
	"math"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/sajari/regression"
)

type ParameterEstimator struct {
	collector      *MetricsCollector
	lastGamma1     float64
	lastGamma2     float64
	lastDelta      float64
	lastUpdate     time.Time
	updateInterval time.Duration
	mutex          sync.RWMutex
}

func NewParameterEstimator(collector *MetricsCollector, updateInterval time.Duration) *ParameterEstimator {
	return &ParameterEstimator{
		collector:      collector,
		lastGamma1:     0.5, // 默认值
		lastGamma2:     1.0, // 默认值
		lastDelta:      0.1, // 默认值
		updateInterval: updateInterval,
	}
}

func (pe *ParameterEstimator) GetCurrentParameters() (float64, float64, float64) {
	pe.mutex.RLock()
	defer pe.mutex.RUnlock()
	return pe.lastGamma1, pe.lastGamma2, pe.lastDelta
}

func (pe *ParameterEstimator) Run() {
	ticker := time.NewTicker(pe.updateInterval)
	defer ticker.Stop()

	for range ticker.C {
		pe.estimateParameters()
	}
}

func (pe *ParameterEstimator) estimateParameters() {
	metrics := pe.collector.GetRecentMetrics(100) // 使用最近100个样本
	if len(metrics) < 20 {
		return // 样本不足时保持当前参数
	}

	// 准备数据帧
	df := dataframe.New(
		series.New(pe.getBatchSizes(metrics), series.Float, "batch_size"),
		series.New(pe.getQueueLengths(metrics), series.Float, "queue_length"),
		series.New(pe.getProcessingTimes(metrics), series.Float, "processing_time"),
		series.New(pe.getBaseLatencies(metrics), series.Float, "base_latency"),
		series.New(pe.getArrivalRates(metrics), series.Float, "arrival_rate"),
	)

	// 估计gamma1和gamma2
	gamma1, gamma2 := pe.estimateGammas(df)

	// 估计delta
	delta := pe.estimateDelta(df, gamma1, gamma2)

	pe.mutex.Lock()
	defer pe.mutex.Unlock()

	pe.lastGamma1 = gamma1
	pe.lastGamma2 = gamma2
	pe.lastDelta = delta
	pe.lastUpdate = time.Now()
}

func (pe *ParameterEstimator) estimateGammas(df dataframe.DataFrame) (float64, float64) {
	r := new(regression.Regression)
	r.SetObserved("processing_time - base_latency")
	r.SetVar(0, "batch_size")
	r.SetVar(1, "queue_length")

	for i := 0; i < df.Nrow(); i++ {
		pt := df.Col("processing_time").Val(i).(float64)
		bl := df.Col("base_latency").Val(i).(float64)
		bs := df.Col("batch_size").Val(i).(float64)
		ql := df.Col("queue_length").Val(i).(float64)

		r.Train(
			regression.DataPoint(pt-bl, []float64{bs, ql}),
		)
	}

	r.Run()

	return r.Coeff(0), r.Coeff(1)
}

func (pe *ParameterEstimator) estimateDelta(df dataframe.DataFrame, gamma1, gamma2 float64) float64 {
	r := new(regression.Regression)
	r.SetObserved("log(1 - efficiency)")
	r.SetVar(0, "batch_size")

	for i := 0; i < df.Nrow(); i++ {
		pt := df.Col("processing_time").Val(i).(float64)
		bl := df.Col("base_latency").Val(i).(float64)
		bs := df.Col("batch_size").Val(i).(float64)
		ql := df.Col("queue_length").Val(i).(float64)
		ar := df.Col("arrival_rate").Val(i).(float64)

		expectedLatency := bl + gamma1*bs + gamma2*ql
		if expectedLatency <= 0 || pt <= 0 || ar <= 0 {
			continue
		}

		efficiency := expectedLatency / (pt * ar) // efficiency = L / (T * λ)
		if efficiency > 0 && efficiency < 1 {
			r.Train(
				regression.DataPoint(math.Log(1-efficiency), []float64{bs}),
			)
		}
	}

	r.Run()
	return -r.Coeff(0) // delta = -slope
}

func (pe *ParameterEstimator) getBatchSizes(metrics []BatchMetrics) []float64 {
	result := make([]float64, len(metrics))
	for i, m := range metrics {
		result[i] = float64(m.BatchSize)
	}
	return result
}

func (pe *ParameterEstimator) getQueueLengths(metrics []BatchMetrics) []float64 {
	result := make([]float64, len(metrics))
	for i, m := range metrics {
		result[i] = float64(m.QueueLength)
	}
	return result
}

func (pe *ParameterEstimator) getProcessingTimes(metrics []BatchMetrics) []float64 {
	result := make([]float64, len(metrics))
	for i, m := range metrics {
		result[i] = m.ProcessingTimeMs
	}
	return result
}

func (pe *ParameterEstimator) getBaseLatencies(metrics []BatchMetrics) []float64 {
	result := make([]float64, len(metrics))
	for i, m := range metrics {
		result[i] = m.BaseLatencyMs
	}
	return result
}

func (pe *ParameterEstimator) getArrivalRates(metrics []BatchMetrics) []float64 {
	result := make([]float64, len(metrics))
	for i, m := range metrics {
		result[i] = m.ArrivalRate
	}
	return result
}
