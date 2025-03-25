// metrics_exporter.go
package batcher

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	batchSizeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_batch_size",
		Help: "Current dynamic batch size",
	})

	queueLengthGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_queue_length",
		Help: "Current request queue length",
	})

	processingTimeGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_processing_time_ms",
		Help: "Last batch processing time in milliseconds",
	})

	baseLatencyGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_base_latency_ms",
		Help: "Current base inference latency (L0)",
	})

	arrivalRateGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_arrival_rate",
		Help: "Current request arrival rate",
	})

	paramGamma1Gauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_param_gamma1",
		Help: "Current gamma1 parameter value",
	})

	paramGamma2Gauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_param_gamma2",
		Help: "Current gamma2 parameter value",
	})

	paramDeltaGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kserve_batcher_param_delta",
		Help: "Current delta parameter value",
	})
)

func (handler *BatchHandler) updatePrometheusMetrics(gamma1, gamma2, delta float64) {
	handler.InfoRwMutex.RLock()
	defer handler.InfoRwMutex.RUnlock()

	r, err := getArrivalRate()
	if err != nil {
		handler.log.Errorf("Failed to get arrival rate: %v", err)
		return
	}
	L0, err := getBaseLatency()
	if err != nil {
		handler.log.Errorf("Failed to get base latency: %v", err)
		return
	}

	batchSizeGauge.Set(float64(handler.BatchSize))
	queueLengthGauge.Set(float64(handler.batcherInfo.CurrentInputLen))
	baseLatencyGauge.Set(L0)
	arrivalRateGauge.Set(r)
	paramGamma1Gauge.Set(gamma1)
	paramGamma2Gauge.Set(gamma2)
	paramDeltaGauge.Set(delta)
}

func (handler *BatchHandler) StartMetricsServer(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			handler.log.Errorf("Metrics server error: %v", err)
		}
	}()
}
