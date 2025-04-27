package batching

import (
	"fmt" // Added for potential error logging
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// --- Constants ---
// (Constants remain the same as before)
const (
	// Default configuration values
	DefaultInitialMaxBatchSize      = 32
	DefaultInitialMaxLatencyMs      = 10 // milliseconds
	DefaultMinBatchSize             = 1
	DefaultMaxBatchSize             = 32   // Adjust if needed
	DefaultMinLatencyMs             = 1    // milliseconds
	DefaultMaxLatencyMs             = 5000 // milliseconds
	DefaultTargetLatencyMs          = 20   // milliseconds (SLO Target)
	DefaultTargetLatencyPercentile  = 0.95 // (95th percentile)
	DefaultLatencyWindowSize        = 100  // Affects avg batch latency, not P2 window
	DefaultQueueLengthWindowSize    = 50
	DefaultPromNamespace            = "kserve"
	DefaultPromSubsystem            = "adaptive_batcher"
	DefaultStateTransitionCoolDownS = 5 // Seconds before allowing another state transition

	// AIMD Control Parameters (Defaults)
	DefaultBatchSizeIncreaseStep = 1
	DefaultLatencyIncreaseStepMs = 1 // milliseconds
	// Decrease factors applied when in Warning/Danger
	DefaultWarningBatchSizeDecreaseFactor = 0.85
	DefaultWarningLatencyDecreaseFactor   = 0.90
	DefaultDangerBatchSizeDecreaseFactor  = 0.70
	DefaultDangerLatencyDecreaseFactor    = 0.75

	// Queue Length Heuristic Threshold
	DefaultQueueLengthIncreaseBatchSizeThreshold = 0.5
)

// BatcherState represents the operational zone based on latency SLO compliance.
type BatcherState int

const (
	StateSafe BatcherState = iota
	StateExplore
	StateWarning
	StateDanger
)

func (s BatcherState) String() string {
	switch s {
	case StateSafe:
		return "Safe"
	case StateExplore:
		return "Explore"
	case StateWarning:
		return "Warning"
	case StateDanger:
		return "Danger"
	default:
		return "Unknown"
	}
}

// --- Configuration ---
// (AdaptiveBatcherConfig struct remains the same)
type AdaptiveBatcherConfig struct {
	// Basic Parameters
	InitialMaxBatchSize int           `json:"initialMaxBatchSize"`
	InitialMaxLatency   time.Duration `json:"initialMaxLatency"`
	MinBatchSize        int           `json:"minBatchSize"`
	MaxBatchSize        int           `json:"maxBatchSize"`
	MinLatency          time.Duration `json:"minLatency"`
	MaxLatency          time.Duration `json:"maxLatency"`

	// SLO & Latency Control
	TargetLatency           time.Duration `json:"targetLatency"`           // The target latency SLO (e.g., 20ms)
	TargetLatencyPercentile float64       `json:"targetLatencyPercentile"` // The target percentile (e.g., 0.95)
	LatencyWindowSize       int           `json:"latencyWindowSize"`       // For *Moving Average* calculation buffer

	// Queue Length Monitoring
	QueueLengthWindowSize int `json:"queueLengthWindowSize"` // For moving average

	// AIMD Control Parameters
	BatchSizeIncreaseStep                 int           `json:"batchSizeIncreaseStep"`
	LatencyIncreaseStep                   time.Duration `json:"latencyIncreaseStep"`
	WarningBatchSizeDecreaseFactor        float64       `json:"warningBatchSizeDecreaseFactor"`
	WarningLatencyDecreaseFactor          float64       `json:"warningLatencyDecreaseFactor"`
	DangerBatchSizeDecreaseFactor         float64       `json:"dangerBatchSizeDecreaseFactor"`
	DangerLatencyDecreaseFactor           float64       `json:"dangerLatencyDecreaseFactor"`
	QueueLengthIncreaseBatchSizeThreshold float64       `json:"queueLengthIncreaseBatchSizeThreshold"` // Heuristic threshold

	// State Transition Control
	StateTransitionCoolDown time.Duration `json:"stateTransitionCoolDown"` // Minimum time between state changes

	// Prometheus Integration
	PrometheusNamespace string            `json:"prometheusNamespace"`
	PrometheusSubsystem string            `json:"prometheusSubsystem"`
	PrometheusLabels    map[string]string `json:"prometheusLabels"` // e.g., {"model_name": "my_model"}
}

// --- Ring Buffer Helper ---
// (FloatRingBuffer remains the same)
type FloatRingBuffer struct {
	buffer []float64
	size   int
	index  int
	sum    float64
	count  int // Number of elements added, up to size
}

func NewFloatRingBuffer(size int) *FloatRingBuffer {
	if size <= 0 {
		size = 1 // Avoid division by zero
	}
	return &FloatRingBuffer{
		buffer: make([]float64, size),
		size:   size,
	}
}

func (rb *FloatRingBuffer) Add(value float64) {
	if rb.count == rb.size {
		// Buffer is full, subtract the oldest value
		rb.sum -= rb.buffer[rb.index]
	} else {
		// Buffer is not full yet
		rb.count++
	}
	rb.buffer[rb.index] = value
	rb.sum += value
	rb.index = (rb.index + 1) % rb.size
}

func (rb *FloatRingBuffer) Average() float64 {
	if rb.count == 0 {
		return 0.0
	}
	return rb.sum / float64(rb.count)
}

// --- Adaptive Batcher ---

// AdaptiveBatcher dynamically adjusts batching parameters based on observed performance.
type AdaptiveBatcher struct {
	config AdaptiveBatcherConfig
	log    *zap.SugaredLogger

	// State
	currentMaxBatchSize int
	currentMaxLatency   time.Duration
	currentState        BatcherState
	lastStateChangeTime time.Time

	// Metrics & Statistics
	latencyEstimator   *P2Estimator     // *** CHANGED TYPE ***
	queueLengthBuffer  *FloatRingBuffer // Moving average of queue length
	batchLatencyBuffer *FloatRingBuffer // Moving average of *batch* latency

	// Prometheus Metrics
	promLabels        prometheus.Labels
	promMaxBatchSize  prometheus.Gauge
	promMaxLatency    prometheus.Gauge
	promCurrentState  prometheus.Gauge
	promLatencyPXX    prometheus.Gauge // PXX = target percentile
	promAvgLatency    prometheus.Gauge
	promAvgQueueLen   prometheus.Gauge
	promBatchLatency  prometheus.Histogram
	promBatchSize     prometheus.Histogram
	promRequestsTotal prometheus.Counter
	promBatchesTotal  prometheus.Counter

	mu sync.RWMutex // Protects access to state and metrics
}

// NewAdaptiveBatcher creates and initializes a new AdaptiveBatcher.
func NewAdaptiveBatcher(config AdaptiveBatcherConfig, logger *zap.SugaredLogger) *AdaptiveBatcher {
	// Apply defaults
	// (Defaults logic remains the same)
	if config.InitialMaxBatchSize <= 0 {
		config.InitialMaxBatchSize = DefaultInitialMaxBatchSize
	}
	if config.InitialMaxLatency <= 0 {
		config.InitialMaxLatency = time.Duration(DefaultInitialMaxLatencyMs) * time.Millisecond
	}
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = DefaultMinBatchSize
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = DefaultMaxBatchSize
	}
	if config.MinLatency <= 0 {
		config.MinLatency = time.Duration(DefaultMinLatencyMs) * time.Millisecond
	}
	if config.MaxLatency <= 0 {
		config.MaxLatency = time.Duration(DefaultMaxLatencyMs) * time.Millisecond
	}
	if config.TargetLatency <= 0 {
		config.TargetLatency = time.Duration(DefaultTargetLatencyMs) * time.Millisecond
	}
	if config.TargetLatencyPercentile <= 0 || config.TargetLatencyPercentile >= 1 {
		config.TargetLatencyPercentile = DefaultTargetLatencyPercentile
	}
	if config.LatencyWindowSize <= 0 {
		config.LatencyWindowSize = DefaultLatencyWindowSize
	}
	if config.QueueLengthWindowSize <= 0 {
		config.QueueLengthWindowSize = DefaultQueueLengthWindowSize
	}
	if config.BatchSizeIncreaseStep <= 0 {
		config.BatchSizeIncreaseStep = DefaultBatchSizeIncreaseStep
	}
	if config.LatencyIncreaseStep <= 0 {
		config.LatencyIncreaseStep = time.Duration(DefaultLatencyIncreaseStepMs) * time.Millisecond
	}
	if config.WarningBatchSizeDecreaseFactor <= 0 || config.WarningBatchSizeDecreaseFactor >= 1 {
		config.WarningBatchSizeDecreaseFactor = DefaultWarningBatchSizeDecreaseFactor
	}
	if config.WarningLatencyDecreaseFactor <= 0 || config.WarningLatencyDecreaseFactor >= 1 {
		config.WarningLatencyDecreaseFactor = DefaultWarningLatencyDecreaseFactor
	}
	if config.DangerBatchSizeDecreaseFactor <= 0 || config.DangerBatchSizeDecreaseFactor >= 1 {
		config.DangerBatchSizeDecreaseFactor = DefaultDangerBatchSizeDecreaseFactor
	}
	if config.DangerLatencyDecreaseFactor <= 0 || config.DangerLatencyDecreaseFactor >= 1 {
		config.DangerLatencyDecreaseFactor = DefaultDangerLatencyDecreaseFactor
	}
	if config.QueueLengthIncreaseBatchSizeThreshold <= 0 {
		config.QueueLengthIncreaseBatchSizeThreshold = DefaultQueueLengthIncreaseBatchSizeThreshold
	}
	if config.StateTransitionCoolDown <= 0 {
		config.StateTransitionCoolDown = time.Duration(DefaultStateTransitionCoolDownS) * time.Second
	}
	if config.PrometheusNamespace == "" {
		config.PrometheusNamespace = DefaultPromNamespace
	}
	if config.PrometheusSubsystem == "" {
		config.PrometheusSubsystem = DefaultPromSubsystem
	}
	if config.PrometheusLabels == nil {
		config.PrometheusLabels = make(map[string]string)
	}

	ab := &AdaptiveBatcher{
		config:              config,
		log:                 logger.Named("adaptive_batcher"),
		currentMaxBatchSize: config.InitialMaxBatchSize,
		currentMaxLatency:   config.InitialMaxLatency,
		currentState:        StateSafe, // Start optimistically
		lastStateChangeTime: time.Now(),
		latencyEstimator:    NewP2Estimator(config.TargetLatencyPercentile),
		queueLengthBuffer:   NewFloatRingBuffer(config.QueueLengthWindowSize),
		batchLatencyBuffer:  NewFloatRingBuffer(config.LatencyWindowSize), // Use same window for avg batch latency
		promLabels:          config.PrometheusLabels,
	}

	ab.createPrometheusMetrics()
	ab.updatePrometheusGauges() // Set initial gauge values

	ab.log.Info("Adaptive Batcher initialized", zap.Any("config", config)) // Use zap.Any for struct logging
	return ab
}

// createPrometheusMetrics initializes the Prometheus metric vectors.
// (Remains the same)
func (ab *AdaptiveBatcher) createPrometheusMetrics() {
	ns := ab.config.PrometheusNamespace
	sub := ab.config.PrometheusSubsystem
	labels := ab.promLabels

	ab.promMaxBatchSize = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "max_batch_size", Help: "Current maximum batch size allowed by the adaptive batcher.", ConstLabels: labels})
	ab.promMaxLatency = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "max_latency_ms", Help: "Current maximum latency (in milliseconds) allowed by the adaptive batcher.", ConstLabels: labels})
	ab.promCurrentState = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "current_state", Help: "Current operational state of the adaptive batcher (0:Safe, 1:Explore, 2:Warning, 3:Danger).", ConstLabels: labels})
	ab.promLatencyPXX = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "estimated_latency_pxx_ms", Help: "Estimated target percentile latency (in milliseconds) based on observations.", ConstLabels: labels})
	ab.promAvgLatency = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "average_batch_latency_ms", Help: "Moving average of observed batch processing latency (in milliseconds).", ConstLabels: labels})
	ab.promAvgQueueLen = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Subsystem: sub, Name: "average_queue_length", Help: "Moving average of the request queue length.", ConstLabels: labels})
	ab.promBatchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "batch_latency_ms_histogram", Help: "Histogram of observed batch processing latencies in milliseconds.", ConstLabels: labels, Buckets: prometheus.ExponentialBuckets(1, 2, 16)})
	ab.promBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Subsystem: sub, Name: "batch_size_histogram", Help: "Histogram of processed batch sizes.", ConstLabels: labels, Buckets: prometheus.LinearBuckets(1, 5, 10)})
	ab.promRequestsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "requests_processed_total", Help: "Total number of individual requests processed.", ConstLabels: labels})
	ab.promBatchesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Subsystem: sub, Name: "batches_processed_total", Help: "Total number of batches processed.", ConstLabels: labels})
}

// GetMetrics returns all Prometheus collectors associated with this batcher.
// (Remains the same)
func (ab *AdaptiveBatcher) GetMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		ab.promMaxBatchSize, ab.promMaxLatency, ab.promCurrentState,
		ab.promLatencyPXX, ab.promAvgLatency, ab.promAvgQueueLen,
		ab.promBatchLatency, ab.promBatchSize, ab.promRequestsTotal, ab.promBatchesTotal,
	}
}

// updatePrometheusGauges updates the Prometheus gauges with current values.
// Assumes appropriate lock (read or write) is held by the caller.
func (ab *AdaptiveBatcher) updatePrometheusGauges() {
	ab.promMaxBatchSize.Set(float64(ab.currentMaxBatchSize))
	ab.promMaxLatency.Set(float64(ab.currentMaxLatency.Milliseconds()))
	ab.promCurrentState.Set(float64(ab.currentState))

	// *** UPDATE: Handle error from Quantile() ***
	pxxValNs, err := ab.latencyEstimator.Quantile()
	pxxValMs := 0.0 // Default value if error or NaN
	if err == nil && !math.IsNaN(pxxValNs) {
		pxxValMs = pxxValNs / 1e6 // Convert ns to ms
	} // Optionally log error if needed: else if err != nil { ab.log.Debugw("Error getting PXX for gauge", zap.Error(err)) }
	ab.promLatencyPXX.Set(pxxValMs)

	ab.promAvgLatency.Set(ab.batchLatencyBuffer.Average() / 1e6) // Convert ns to ms
	ab.promAvgQueueLen.Set(ab.queueLengthBuffer.Average())
}

// GetBatchingParams returns the current dynamically adjusted batch size and latency.
// (Remains the same)
func (ab *AdaptiveBatcher) GetBatchingParams() (int, time.Duration) {
	ab.mu.RLock()
	defer ab.mu.RUnlock()
	return ab.currentMaxBatchSize, ab.currentMaxLatency
}

// ObserveBatch records the performance of a completed batch.
// latency: The total time taken for the batch (from first request arrival to response dispatch).
func (ab *AdaptiveBatcher) ObserveBatch(batchSize int, latency time.Duration) {
	if batchSize <= 0 {
		return // Ignore empty batches
	}

	ab.mu.Lock() // Lock for modifying state and metrics
	defer ab.mu.Unlock()

	latencyNs := float64(latency.Nanoseconds())

	// Update metrics
	ab.latencyEstimator.Add(latencyNs)
	ab.batchLatencyBuffer.Add(latencyNs)

	// Update Prometheus counters and histograms
	ab.promRequestsTotal.Add(float64(batchSize))
	ab.promBatchesTotal.Inc()
	ab.promBatchSize.Observe(float64(batchSize))
	ab.promBatchLatency.Observe(latency.Seconds() * 1000) // Observe latency in milliseconds

	// Adjust parameters based on the new observation
	ab.adjustParams() // This function MUST be called while Lock is held

	// Update gauges after adjustment (still under lock)
	ab.updatePrometheusGauges()

	// --- Enhanced Debug Logging ---
	// Get PXX value again *after* potential adjustment for logging
	pxxValNs, pxxErr := ab.latencyEstimator.Quantile()
	pxxLogVal := "error"
	if pxxErr == nil {
		pxxLogVal = fmt.Sprintf("%.2fms", pxxValNs/1e6)
	}

	ab.log.Debug("Observed batch & Adjusted state",
		zap.Int("batchSize", batchSize),
		zap.Duration("latency", latency),
		zap.String("pXXLatency", pxxLogVal), // Log PXX estimate
		zap.Float64("avgLatencyMs", ab.batchLatencyBuffer.Average()/1e6),
		zap.Int("newMaxBatchSize", ab.currentMaxBatchSize),
		zap.Duration("newMaxLatency", ab.currentMaxLatency),
		zap.Stringer("newState", ab.currentState),
	)
	// --- End Enhanced Debug Logging ---
}

// ObserveQueueLength records the current queue length.
func (ab *AdaptiveBatcher) ObserveQueueLength(queueLength int) {
	ab.mu.Lock()
	defer ab.mu.Unlock()
	ab.queueLengthBuffer.Add(float64(queueLength))
	// Update gauge immediately if desired
	// ab.promAvgQueueLen.Set(ab.queueLengthBuffer.Average())
}

// adjustParams implements the core AIMD logic based on the current state.
// This function MUST be called with the write mutex held.
func (ab *AdaptiveBatcher) adjustParams() {
	now := time.Now()
	// Only adjust if cool-down period has passed since last state change
	if now.Sub(ab.lastStateChangeTime) < ab.config.StateTransitionCoolDown {
		return
	}

	// *** UPDATE: Get PXX estimate and handle error ***
	pXXLatencyNs, err := ab.latencyEstimator.Quantile()
	if err != nil {
		ab.log.Warnw("Failed to get PXX latency estimate, skipping adjustment", zap.Error(err))
		return // Cannot adjust without estimate
	}
	// Extra check for safety, though Quantile() tries to avoid NaN
	if math.IsNaN(pXXLatencyNs) {
		ab.log.Warn("PXX latency is NaN, skipping adjustment")
		return
	}

	targetLatencyNs := float64(ab.config.TargetLatency.Nanoseconds())
	avgQueueLen := ab.queueLengthBuffer.Average() // Read average queue length

	// Define state thresholds based on PXX latency vs target
	// (Thresholds remain the same)
	dangerThreshold := targetLatencyNs
	warningThreshold := targetLatencyNs * 0.9 // Enter warning if PXX > 90% of target
	exploreThreshold := targetLatencyNs * 0.7 // Enter explore if PXX > 70% of target
	safeThreshold := targetLatencyNs * 0.65   // Re-enter safe if PXX < 65% of target

	previousState := ab.currentState
	newState := previousState

	// Determine new state based on PXX latency
	// (State transition logic remains the same)
	if pXXLatencyNs > dangerThreshold {
		newState = StateDanger
	} else if pXXLatencyNs > warningThreshold {
		newState = StateWarning
	} else if pXXLatencyNs > exploreThreshold {
		if previousState == StateSafe || previousState == StateExplore {
			newState = StateExplore
		}
	} else if pXXLatencyNs < safeThreshold {
		if previousState == StateWarning || previousState == StateDanger {
			newState = StateExplore
		} else {
			newState = StateSafe
		}
	} // else: Remain in hysteresis gap

	if newState != previousState {
		ab.log.Info("Batcher state transition",
			zap.Stringer("from", previousState),
			zap.Stringer("to", newState),
			zap.Float64("pXXLatencyMs", pXXLatencyNs/1e6),
			zap.Float64("targetLatencyMs", targetLatencyNs/1e6),
		)
		ab.currentState = newState
		ab.lastStateChangeTime = now
	}

	// --- Apply AIMD adjustments based on the *new* state ---
	// (AIMD logic remains the same)
	prevBatchSize := ab.currentMaxBatchSize
	prevLatency := ab.currentMaxLatency

	switch ab.currentState {
	case StateDanger:
		ab.currentMaxBatchSize = maxInt(ab.config.MinBatchSize, int(float64(prevBatchSize)*ab.config.DangerBatchSizeDecreaseFactor))
		ab.currentMaxLatency = maxDuration(ab.config.MinLatency, time.Duration(float64(prevLatency.Nanoseconds())*ab.config.DangerLatencyDecreaseFactor))
	case StateWarning:
		ab.currentMaxBatchSize = maxInt(ab.config.MinBatchSize, int(float64(prevBatchSize)*ab.config.WarningBatchSizeDecreaseFactor))
		ab.currentMaxLatency = maxDuration(ab.config.MinLatency, time.Duration(float64(prevLatency.Nanoseconds())*ab.config.WarningLatencyDecreaseFactor))
	case StateExplore, StateSafe:
		increaseBatchSize := avgQueueLen > ab.config.QueueLengthIncreaseBatchSizeThreshold*float64(prevBatchSize) // Use prevBatchSize for consistency within this step
		increasedB := false
		if increaseBatchSize && prevBatchSize < ab.config.MaxBatchSize {
			ab.currentMaxBatchSize = minInt(ab.config.MaxBatchSize, prevBatchSize+ab.config.BatchSizeIncreaseStep)
			increasedB = true
		} else {
			ab.currentMaxBatchSize = prevBatchSize // Ensure it doesn't change if not increasing
		}

		// Increase Latency only if Batch Size wasn't increased OR Batch Size is already at Max
		if (!increasedB || ab.currentMaxBatchSize == ab.config.MaxBatchSize) && prevLatency < ab.config.MaxLatency {
			ab.currentMaxLatency = minDuration(ab.config.MaxLatency, prevLatency+ab.config.LatencyIncreaseStep)
		} else {
			ab.currentMaxLatency = prevLatency // Ensure it doesn't change if not increasing
		}
	}

	// --- Final clamping and logging ---
	// Clamp final values just in case factors resulted in out-of-bounds
	ab.currentMaxBatchSize = maxInt(ab.config.MinBatchSize, minInt(ab.config.MaxBatchSize, ab.currentMaxBatchSize))
	ab.currentMaxLatency = maxDuration(ab.config.MinLatency, minDuration(ab.config.MaxLatency, ab.currentMaxLatency))

	if ab.currentMaxBatchSize != prevBatchSize || ab.currentMaxLatency != prevLatency {
		ab.log.Debug("Adjusted parameters",
			zap.Stringer("state", ab.currentState),
			zap.Int("oldBatchSize", prevBatchSize),
			zap.Int("newBatchSize", ab.currentMaxBatchSize),
			zap.Duration("oldLatency", prevLatency),
			zap.Duration("newLatency", ab.currentMaxLatency),
			zap.Float64("avgQueueLen", avgQueueLen),
		)
	}
}

// --- Utility Functions ---
// (minInt, maxInt, minDuration, maxDuration remain the same)
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
