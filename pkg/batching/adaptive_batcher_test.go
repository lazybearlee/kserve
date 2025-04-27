package batching

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// Helper to create a default config for testing
func newTestConfig() AdaptiveBatcherConfig {
	// Use values that make thresholds distinct for easier testing
	return AdaptiveBatcherConfig{
		InitialMaxBatchSize:                   10,
		InitialMaxLatency:                     20 * time.Millisecond,
		MinBatchSize:                          1,
		MaxBatchSize:                          50,
		MinLatency:                            1 * time.Millisecond,
		MaxLatency:                            100 * time.Millisecond,
		TargetLatency:                         20 * time.Millisecond, // Target SLO: 20ms
		TargetLatencyPercentile:               0.95,
		LatencyWindowSize:                     10, // Affects avg buffer, NOT P2 window size
		QueueLengthWindowSize:                 5,
		BatchSizeIncreaseStep:                 1,
		LatencyIncreaseStep:                   1 * time.Millisecond,
		WarningBatchSizeDecreaseFactor:        0.8,
		WarningLatencyDecreaseFactor:          0.9,
		DangerBatchSizeDecreaseFactor:         0.6,
		DangerLatencyDecreaseFactor:           0.7,
		QueueLengthIncreaseBatchSizeThreshold: 0.5,
		StateTransitionCoolDown:               1 * time.Millisecond, // Very short cool-down for testing
		PrometheusNamespace:                   "test_ns",
		PrometheusSubsystem:                   "test_sub",
		PrometheusLabels:                      map[string]string{"test": "true"},
	}
}

// Helper to create a batcher for testing - Uses the latest P2Estimator
func newTestAdaptiveBatcher(t *testing.T, config AdaptiveBatcherConfig) *AdaptiveBatcher {
	logger := zaptest.NewLogger(t)
	sugaredLogger := logger.Sugar()
	ab := NewAdaptiveBatcher(config, sugaredLogger) // Creates P2Estimator internally
	require.NotNil(t, ab)

	// Use a latency well below the safe threshold (target * 0.65 = 13ms)
	initLatency := config.TargetLatency / 4 // 5ms
	if initLatency < config.MinLatency {
		initLatency = config.MinLatency
	}

	// Add minimum samples to initialize P2 estimator with consistent low latency
	for i := 0; i < numMarkers; i++ { // Use constant numMarkers from P2Estimator
		ab.ObserveBatch(1, initLatency)
	}

	// Reset state/time after initial observations for clean test start
	ab.mu.Lock()
	ab.currentState = StateSafe
	ab.lastStateChangeTime = time.Now()
	ab.currentMaxBatchSize = config.InitialMaxBatchSize
	ab.currentMaxLatency = config.InitialMaxLatency
	// Optional: Reset buffers if desired, P2 was just created.
	ab.queueLengthBuffer = NewFloatRingBuffer(config.QueueLengthWindowSize)
	ab.batchLatencyBuffer = NewFloatRingBuffer(config.LatencyWindowSize)
	ab.mu.Unlock()

	// Verify P2 is initialized
	ab.mu.RLock()
	_, err := ab.latencyEstimator.Quantile()
	ab.mu.RUnlock()
	require.NoError(t, err, "P2Estimator failed to initialize in helper")
	require.True(t, ab.latencyEstimator.initialized, "P2Estimator should be marked initialized")

	return ab
}

// Test Initialization
func TestAdaptiveBatcherInitialization(t *testing.T) {
	config := newTestConfig()
	logger := zaptest.NewLogger(t)
	sugaredLogger := logger.Sugar()
	ab := NewAdaptiveBatcher(config, sugaredLogger)
	require.NotNil(t, ab)

	ab.mu.RLock() // Use RLock for reading state
	assert.Equal(t, config.InitialMaxBatchSize, ab.currentMaxBatchSize)
	assert.Equal(t, config.InitialMaxLatency, ab.currentMaxLatency)
	assert.Equal(t, StateSafe, ab.currentState) // Initial state
	ab.mu.RUnlock()

	assert.NotNil(t, ab.latencyEstimator) // Check P2Estimator field exists
	assert.NotNil(t, ab.queueLengthBuffer)
	assert.NotNil(t, ab.batchLatencyBuffer)

	metrics := ab.GetMetrics()
	assert.GreaterOrEqual(t, len(metrics), 8)

	// Check initial gauge values immediately after creation
	ab.mu.RLock()
	assert.Equal(t, float64(config.InitialMaxBatchSize), testutil.ToFloat64(ab.promMaxBatchSize))
	assert.Equal(t, float64(config.InitialMaxLatency.Milliseconds()), testutil.ToFloat64(ab.promMaxLatency))
	ab.mu.RUnlock()
}

// Test GetParams
func TestAdaptiveBatcherGetParams(t *testing.T) {
	config := newTestConfig()
	ab := newTestAdaptiveBatcher(t, config)
	b, l := ab.GetBatchingParams()
	assert.Equal(t, config.InitialMaxBatchSize, b)
	assert.Equal(t, config.InitialMaxLatency, l)
}

// Test Safe Zone
func TestAdaptiveBatcherObserveAndAdjust_SafeZone(t *testing.T) {
	config := newTestConfig()
	ab := newTestAdaptiveBatcher(t, config)
	initialB, initialL := ab.GetBatchingParams()

	// Simulate consistently good latency (well below target)
	goodLatency := config.TargetLatency / 4 // 5ms
	if goodLatency < config.MinLatency {
		goodLatency = config.MinLatency
	}

	for i := 0; i < 5; i++ { // Observe multiple times to see increase
		ab.ObserveQueueLength(0)                                        // Ensure queue length is 0
		ab.ObserveBatch(initialB, goodLatency)                          // Observe with initial batch size
		time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond) // Ensure cooldown passes
	}

	finalB, finalL := ab.GetBatchingParams()
	ab.mu.RLock()
	finalState := ab.currentState
	pxxVal, pxxErr := ab.latencyEstimator.Quantile()
	ab.mu.RUnlock()
	require.NoError(t, pxxErr, "PXX Quantile() returned error")

	assert.Equal(t, StateSafe, finalState, "Should remain in Safe state")
	// With zero queue length, batch size should not increase, latency should
	assert.Equal(t, initialB, finalB, "Batch size should not increase with zero queue length")
	assert.Greater(t, finalL, initialL, "Latency should increase in Safe zone with zero queue length")

	// Check Prometheus metrics
	assert.Equal(t, float64(finalB), testutil.ToFloat64(ab.promMaxBatchSize))
	assert.Equal(t, float64(finalL.Milliseconds()), testutil.ToFloat64(ab.promMaxLatency))
	assert.Equal(t, float64(StateSafe), testutil.ToFloat64(ab.promCurrentState))

	// PXX should be close to goodLatency
	pxxLatencyMs := pxxVal / 1e6
	t.Logf("SafeZone Test - PXX Latency: %.2fms, Good Latency: %dms", pxxLatencyMs, goodLatency.Milliseconds())
	require.False(t, pxxLatencyMs <= 0 || math.IsNaN(pxxLatencyMs), "PXX latency seems invalid: %.2f", pxxLatencyMs)
	assert.InDelta(t, float64(goodLatency.Milliseconds()), pxxLatencyMs, defaultTestTolerance*2, "PXX latency should be close to observed good latency") // Increased tolerance

	// Average Latency check
	assert.InDelta(t, float64(goodLatency.Milliseconds()), testutil.ToFloat64(ab.promAvgLatency), float64(goodLatency.Milliseconds())*0.5)

	// Counter checks
	batchesTotal := testutil.ToFloat64(ab.promBatchesTotal)
	requestsTotal := testutil.ToFloat64(ab.promRequestsTotal)
	expectedBatches := float64(numMarkers + 5)             // 5 from init + 5 from test loop
	expectedRequests := float64(numMarkers*1 + 5*initialB) // 5x1 from init + 5xinitialB from loop
	assert.InDelta(t, expectedBatches, batchesTotal, 1.0, "Batches total mismatch")
	assert.InDelta(t, expectedRequests, requestsTotal, 1.0, "Requests total mismatch")
}

// Test Danger Zone
func TestAdaptiveBatcherObserveAndAdjust_DangerZone(t *testing.T) {
	config := newTestConfig()
	// Use a slightly larger window/config if needed for P2 stability under stress
	// config.LatencyWindowSize = 10 // Affects avg buffer, not P2
	ab := newTestAdaptiveBatcher(t, config)

	// Start parameters high to observe reduction
	ab.mu.Lock()
	ab.currentMaxBatchSize = config.MaxBatchSize
	ab.currentMaxLatency = config.MaxLatency
	ab.mu.Unlock()
	initialB, initialL := ab.GetBatchingParams()

	// Simulate consistently bad latency (well above target)
	badLatency := config.TargetLatency * 3 // e.g., 60ms

	for i := 0; i < 15; i++ { // Observe more times for P2 to converge upwards
		ab.mu.RLock()
		currentB := ab.currentMaxBatchSize // Observe using the (potentially decreasing) batch size
		ab.mu.RUnlock()
		ab.ObserveBatch(maxInt(1, currentB), badLatency)
		time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
	}

	finalB, finalL := ab.GetBatchingParams()
	ab.mu.RLock()
	finalState := ab.currentState
	pxxValNs, pxxErr := ab.latencyEstimator.Quantile()
	ab.mu.RUnlock()
	require.NoError(t, pxxErr, "PXX Quantile() returned error")
	pxxLatencyMs := pxxValNs / 1e6
	targetMs := float64(config.TargetLatency.Milliseconds())

	t.Logf("DangerZone Test - Final state: %s, PXX Latency: %.2fms, Target: %.2fms", finalState, pxxLatencyMs, targetMs)

	// Assert PXX is high enough and state is Danger
	require.False(t, math.IsNaN(pxxLatencyMs) || pxxLatencyMs <= 0, "PXX latency is invalid: %.2f", pxxLatencyMs)
	// PXX might take time to reach exactly badLatency, check it crossed the threshold
	assert.GreaterOrEqual(t, pxxLatencyMs, targetMs, "PXX latency should be >= target to trigger Danger")
	assert.Equal(t, StateDanger, finalState, "Should end in Danger state")

	// Assert parameters decreased
	assert.Less(t, finalB, initialB, "Batch size should decrease in Danger zone")
	assert.Less(t, finalL, initialL, "Latency should decrease in Danger zone")
	assert.GreaterOrEqual(t, finalB, config.MinBatchSize, "Batch size shouldn't go below min")
	assert.GreaterOrEqual(t, finalL, config.MinLatency, "Latency shouldn't go below min")

	// Check Prometheus
	assert.Equal(t, float64(finalB), testutil.ToFloat64(ab.promMaxBatchSize))
	assert.Equal(t, float64(finalL.Milliseconds()), testutil.ToFloat64(ab.promMaxLatency))
	assert.Equal(t, float64(StateDanger), testutil.ToFloat64(ab.promCurrentState))
}

// Test State Transitions
func TestAdaptiveBatcherStateTransitions(t *testing.T) {
	config := newTestConfig()
	// config.LatencyWindowSize = 5 // Affects avg buffer only
	ab := newTestAdaptiveBatcher(t, config)

	targetMs := float64(config.TargetLatency.Milliseconds())
	// Define latencies relative to target
	goodLatency := time.Duration(targetMs*0.5) * time.Millisecond     // ~10ms -> Safe
	warningLatency := time.Duration(targetMs*0.95) * time.Millisecond // ~19ms -> Warning (pXX > 0.9*T)
	dangerLatency := time.Duration(targetMs*1.5) * time.Millisecond   // ~30ms -> Danger (pXX > 1.0*T)
	exploreLatency := time.Duration(targetMs*0.8) * time.Millisecond  // ~16ms -> Explore (pXX > 0.7*T but < 0.9*T)
	safeLatency := time.Duration(targetMs*0.60) * time.Millisecond    // ~12ms -> Safe (pXX < 0.65*T)

	observe := func(latency time.Duration, count int, description string) {
		t.Logf("--- Observing %s (%v x %d) ---", description, latency, count)
		for i := 0; i < count; i++ {
			ab.mu.RLock()
			b := ab.currentMaxBatchSize
			ab.mu.RUnlock()
			ab.ObserveBatch(maxInt(1, b), latency)
			time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
		}
		ab.mu.RLock()
		currState := ab.currentState
		currB := ab.currentMaxBatchSize
		currL := ab.currentMaxLatency
		pxx, pxxErr := ab.latencyEstimator.Quantile()
		ab.mu.RUnlock()
		pxxMs := 0.0
		if pxxErr == nil && !math.IsNaN(pxx) {
			pxxMs = pxx / 1e6
		}
		t.Logf("After %s: State=%s, PXX=%.2fms, B=%d, L=%v", description, currState, pxxMs, currB, currL)
	}

	// --- Sequence of Observations ---
	// 1. Start Safe
	ab.mu.RLock()
	require.Equal(t, StateSafe, ab.currentState)
	ab.mu.RUnlock()
	observe(goodLatency, 5, "Good Latency (Stay Safe)")
	ab.mu.RLock()
	assert.Equal(t, StateSafe, ab.currentState)
	ab.mu.RUnlock()

	// 2. Safe -> Explore -> Warning
	observe(exploreLatency, 10, "Explore Latency (Safe->Explore)") // Need enough obs for P2 to rise
	ab.mu.RLock()
	assert.Equal(t, StateExplore, ab.currentState)
	ab.mu.RUnlock()
	observe(warningLatency, 10, "Warning Latency (Explore->Warning)")
	ab.mu.RLock()
	assert.Equal(t, StateWarning, ab.currentState)
	ab.mu.RUnlock()
	ab.mu.RLock()
	initialWarningL := ab.currentMaxLatency
	ab.mu.RUnlock() // Get L while in Warning

	// 3. Warning -> Danger
	observe(dangerLatency, 10, "Danger Latency (Warning->Danger)")
	ab.mu.RLock()
	assert.Equal(t, StateDanger, ab.currentState)
	ab.mu.RUnlock()
	ab.mu.RLock()
	dangerL := ab.currentMaxLatency
	ab.mu.RUnlock()
	assert.Less(t, dangerL, initialWarningL, "Latency should decrease entering Danger from Warning")

	// 4. Danger -> Explore (via good latency)
	observe(goodLatency, 10, "Good Latency (Danger->Explore)")
	ab.mu.RLock()
	pxxDangerExit, pxxErr := ab.latencyEstimator.Quantile()
	require.NoError(t, pxxErr)
	stateDangerExit := ab.currentState
	exploreL := ab.currentMaxLatency
	ab.mu.RUnlock()
	t.Logf("Exiting Danger: State=%s, PXX=%.2fms", stateDangerExit, pxxDangerExit/1e6)
	assert.Equal(t, StateExplore, stateDangerExit) // Drops to Explore first
	assert.LessOrEqual(t, pxxDangerExit/1e6, targetMs*0.9, "PXX should drop below Warning threshold")
	// Latency might start increasing in Explore, but should still be lower than when Warning was entered
	assert.Less(t, exploreL, initialWarningL, "Latency should still be relatively low after exiting Danger")

	// 5. Explore -> Safe
	observe(safeLatency, 10, "Safe Latency (Explore->Safe)")
	ab.mu.RLock()
	assert.Equal(t, StateSafe, ab.currentState)
	ab.mu.RUnlock()
}

// Test Clamping
func TestAdaptiveBatcherParameterClamping(t *testing.T) {
	config := newTestConfig()
	config.MinBatchSize = 5
	config.MaxBatchSize = 15
	config.MinLatency = 10 * time.Millisecond
	config.MaxLatency = 30 * time.Millisecond
	config.InitialMaxBatchSize = 10
	config.InitialMaxLatency = 20 * time.Millisecond
	config.TargetLatency = 100 * time.Millisecond // High target to force increase

	ab := newTestAdaptiveBatcher(t, config)

	// Force increase
	goodLatency := 1 * time.Millisecond
	for i := 0; i < 30; i++ { // More observations might be needed
		ab.mu.RLock()
		b := ab.currentMaxBatchSize
		ab.mu.RUnlock()
		ab.ObserveBatch(maxInt(1, b), goodLatency)
		time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
	}
	b, l := ab.GetBatchingParams()
	assert.Equal(t, config.MaxBatchSize, b, "Batch size should be clamped at max")
	assert.Equal(t, config.MaxLatency, l, "Latency should be clamped at max")

	// Force decrease
	ab.mu.Lock()
	ab.currentState = StateDanger // Force danger state
	// Set params high again to observe decrease clearly
	ab.currentMaxBatchSize = config.MaxBatchSize
	ab.currentMaxLatency = config.MaxLatency
	ab.mu.Unlock()

	badLatency := 500 * time.Millisecond
	for i := 0; i < 30; i++ { // More observations might be needed
		ab.mu.RLock()
		bCurr := ab.currentMaxBatchSize
		ab.mu.RUnlock()
		ab.ObserveBatch(maxInt(1, bCurr), badLatency)
		time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
	}
	b, l = ab.GetBatchingParams()
	assert.Equal(t, config.MinBatchSize, b, "Batch size should be clamped at min")
	assert.Equal(t, config.MinLatency, l, "Latency should be clamped at min")
}

// Test Queue Length Heuristic
func TestAdaptiveBatcherQueueLengthHeuristic(t *testing.T) {
	config := newTestConfig()
	config.QueueLengthIncreaseBatchSizeThreshold = 0.5
	config.BatchSizeIncreaseStep = 1
	config.LatencyIncreaseStep = 10 * time.Millisecond // Make L step large
	ab := newTestAdaptiveBatcher(t, config)

	// Ensure state is Safe/Explore
	ab.mu.Lock()
	ab.currentState = StateSafe
	ab.currentMaxBatchSize = 10
	ab.currentMaxLatency = 20 * time.Millisecond
	ab.mu.Unlock()
	initialB, initialL := ab.GetBatchingParams()

	// Case 1: Low Queue Length -> Increase Latency primarily
	ab.ObserveQueueLength(2) // Avg QL will be low
	ab.ObserveQueueLength(3)
	time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
	ab.ObserveBatch(initialB, config.TargetLatency/2) // Good latency
	b1, l1 := ab.GetBatchingParams()
	t.Logf("Low QL - Initial B/L: %d/%v -> Final B/L: %d/%v", initialB, initialL, b1, l1)

	assert.Equal(t, initialB, b1, "Batch size should not increase with low queue length")
	assert.Greater(t, l1, initialL, "Latency should increase with low queue length")

	// Case 2: High Queue Length -> Increase Batch Size primarily
	ab.mu.Lock() // Reset params for clearer comparison
	ab.currentMaxBatchSize = 10
	ab.currentMaxLatency = 20 * time.Millisecond
	ab.currentState = StateSafe // Ensure still safe
	ab.mu.Unlock()
	baseB, baseL := ab.GetBatchingParams() // Use these as base for high QL case

	ab.ObserveQueueLength(8) // Avg QL will be high (relative to B=10)
	ab.ObserveQueueLength(9)
	time.Sleep(config.StateTransitionCoolDown + 1*time.Millisecond)
	ab.ObserveBatch(baseB, config.TargetLatency/2) // Good latency
	b2, l2 := ab.GetBatchingParams()
	t.Logf("High QL - Base B/L: %d/%v -> Final B/L: %d/%v", baseB, baseL, b2, l2)

	assert.Greater(t, b2, baseB, "Batch size should increase with high queue length")
	// Latency might also increase if both allowed, or stay same if B prioritized
	assert.GreaterOrEqual(t, l2, baseL, "Latency might also increase or stay the same")

	assert.Equal(t, float64(b2), testutil.ToFloat64(ab.promMaxBatchSize))
	// Check queue length average (samples: 2, 3, 8, 9; window 5) -> (2+3+8+9)/4 = 5.5 approx
	assert.InDelta(t, (2.0+3.0+8.0+9.0)/4.0, testutil.ToFloat64(ab.promAvgQueueLen), 2.0)
}

// Test Zero Batch
func TestAdaptiveBatcherObserveZeroBatch(t *testing.T) {
	config := newTestConfig()
	ab := newTestAdaptiveBatcher(t, config)
	initialB, initialL := ab.GetBatchingParams()

	// Wait briefly after init helper observations
	time.Sleep(5 * time.Millisecond)
	initialBatches := testutil.ToFloat64(ab.promBatchesTotal)
	initialRequests := testutil.ToFloat64(ab.promRequestsTotal)
	t.Logf("Initial Batches: %.0f, Requests: %.0f", initialBatches, initialRequests)

	ab.ObserveBatch(0, 10*time.Millisecond) // Observe batch with size 0

	b, l := ab.GetBatchingParams()
	assert.Equal(t, initialB, b, "Parameters should not change for zero batch size")
	assert.Equal(t, initialL, l, "Parameters should not change for zero batch size")

	time.Sleep(5 * time.Millisecond) // Allow potential metric updates
	finalBatches := testutil.ToFloat64(ab.promBatchesTotal)
	finalRequests := testutil.ToFloat64(ab.promRequestsTotal)
	t.Logf("Final Batches: %.0f, Requests: %.0f", finalBatches, finalRequests)

	assert.Equal(t, initialBatches, finalBatches, "Batches total should not increment")
	assert.Equal(t, initialRequests, finalRequests, "Requests total should not increment")
}
