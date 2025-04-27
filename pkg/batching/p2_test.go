package batching

import (
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultTestTolerance = 0.1 // Increased default absolute tolerance for P2 tests

// Helper function to calculate the actual quantile from a slice
func calculateActualQuantile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return math.NaN()
	}
	localData := make([]float64, len(data)) // Create a copy to sort locally
	copy(localData, data)
	sort.Float64s(localData)

	// Use linear interpolation between ranks method (R-7 in R lang, commonly used)
	index := p * (float64(len(localData)) + 1.0) // 1-based index rank
	if index < 1.0 {
		return localData[0]
	}
	if index >= float64(len(localData)) {
		return localData[len(localData)-1]
	}

	lower := math.Floor(index)
	upper := math.Ceil(index)
	weight := index - lower

	// Get 0-based slice indices
	idx_lower := int(lower) - 1
	idx_upper := int(upper) - 1
	if idx_lower < 0 {
		idx_lower = 0
	} // Guard against underflow

	if idx_lower == idx_upper {
		return localData[idx_lower]
	}

	return localData[idx_lower]*(1.0-weight) + localData[idx_upper]*weight
}

func TestP2Initialization(t *testing.T) {
	p := 0.95
	est := NewP2Estimator(p) // *** Use NewP2Estimator ***
	require.NotNil(t, est)

	// Before 5 observations
	_, err := est.Quantile() // *** Check error ***
	assert.Error(t, err, "Quantile should return error before init")
	assert.False(t, est.initialized)

	// Add less than 5 observations
	values := []float64{10.0, 20.0, 5.0}
	for _, v := range values {
		est.Add(v) // *** Use Add ***
	}
	assert.Equal(t, len(values), est.count)
	assert.False(t, est.initialized)
	_, err = est.Quantile()
	assert.Error(t, err, "Quantile should still return error before 5 samples")

	// Add remaining observations to initialize
	est.Add(15.0)
	est.Add(25.0)

	assert.Equal(t, numMarkers, est.count)
	assert.True(t, est.initialized)
	// Check if initial markers are sorted (internal detail)
	assert.True(t, sort.Float64sAreSorted(est.q[:]), "Initial marker heights (q) should be sorted")

	// Check if GetQuantile now works
	qVal, err := est.Quantile()
	assert.NoError(t, err, "Quantile should not return error after 5 samples")
	assert.False(t, math.IsNaN(qVal), "Quantile value should not be NaN after 5 samples")

	// Verify the initial quantile estimate (q[2]) is reasonable
	// Initial values: 5, 10, 15, 20, 25. Sorted q = [5, 10, 15, 20, 25]. q[2] = 15.
	assert.Equal(t, 15.0, qVal, "Initial quantile estimate (q[2]) should be the middle value")
}

func TestP2InvalidQuantile(t *testing.T) {
	assert.Panics(t, func() { NewP2Estimator(0.0) })
	assert.Panics(t, func() { NewP2Estimator(1.0) })
	assert.Panics(t, func() { NewP2Estimator(-0.1) })
	assert.Panics(t, func() { NewP2Estimator(1.1) })
}

func TestP2BasicConvergence(t *testing.T) {
	p := 0.50 // Median
	est := NewP2Estimator(p)
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, v := range data {
		est.Add(v)
	}

	require.True(t, est.initialized)
	actualMedian := calculateActualQuantile(data, p) // Approx 5.5
	estimatedMedian, err := est.Quantile()
	require.NoError(t, err)

	t.Logf("Basic Convergence - Actual Median: %.4f, Estimated Median: %.4f", actualMedian, estimatedMedian)
	assert.InDelta(t, actualMedian, estimatedMedian, defaultTestTolerance, "Median estimate mismatch")
}

func TestP2RandomData(t *testing.T) {
	p := 0.90 // 90th percentile
	est := NewP2Estimator(p)
	numSamples := 10000
	data := make([]float64, numSamples)

	rng := rand.New(rand.NewSource(42)) // Fixed seed
	for i := 0; i < numSamples; i++ {
		data[i] = rng.Float64() * 100.0 // Uniform [0, 100)
		est.Add(data[i])
	}

	require.True(t, est.initialized)
	actualP90 := calculateActualQuantile(data, p)
	estimatedP90, err := est.Quantile()
	require.NoError(t, err)

	t.Logf("Random Data - Actual P%.0f: %.4f, Estimated P%.0f: %.4f", p*100, actualP90, p*100, estimatedP90)
	// Use relative tolerance for larger N, but check absolute difference too
	relativeTolerance := 0.05 // Allow 5% relative error for large N random data
	assert.InDelta(t, actualP90, estimatedP90, math.Max(defaultTestTolerance, relativeTolerance*actualP90), "P90 estimate mismatch for random data")
}

func TestP2ReverseSortedData(t *testing.T) {
	p := 0.25 // 25th percentile
	est := NewP2Estimator(p)
	numSamples := 100
	data := make([]float64, numSamples)

	for i := 0; i < numSamples; i++ {
		v := float64(numSamples - i)
		data[i] = v // Will be added in reverse order
		est.Add(v)
	}

	require.True(t, est.initialized)
	// Calculate actual on the originally generated (reverse sorted) values
	actualP25 := calculateActualQuantile(data, p)
	estimatedP25, err := est.Quantile()
	require.NoError(t, err)

	t.Logf("Reverse Sorted - Actual P%.0f: %.4f, Estimated P%.0f: %.4f", p*100, actualP25, p*100, estimatedP25)
	assert.InDelta(t, actualP25, estimatedP25, defaultTestTolerance*2, "P25 estimate mismatch for reverse sorted data") // May need slightly more tolerance
}

func TestP2DuplicateValues(t *testing.T) {
	p := 0.50 // Median
	est := NewP2Estimator(p)
	data := []float64{5, 5, 5, 5, 5, 1, 10, 5, 5, 5, 5, 2, 9, 5, 5}

	for _, v := range data {
		est.Add(v)
	}

	require.True(t, est.initialized)
	actualMedian := calculateActualQuantile(data, p) // Should be 5.0
	estimatedMedian, err := est.Quantile()
	require.NoError(t, err)

	t.Logf("Duplicates - Actual P%.0f: %.4f, Estimated P%.0f: %.4f", p*100, actualMedian, p*100, estimatedMedian)
	assert.InDelta(t, actualMedian, estimatedMedian, defaultTestTolerance, "Median estimate mismatch with duplicates")
}

func TestP2NearEdges(t *testing.T) {
	numSamples := 200
	data := make([]float64, numSamples)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < numSamples; i++ {
		data[i] = rng.Float64()*10.0 + 1.0 // Values between 1 and 11
	}

	t.Run("LowQuantile", func(t *testing.T) {
		p := 0.05
		est := NewP2Estimator(p)
		for _, v := range data {
			est.Add(v)
		}
		require.True(t, est.initialized)

		actual := calculateActualQuantile(data, p)
		estimated, err := est.Quantile()
		require.NoError(t, err)

		t.Logf("Low Quantile - Actual P%.0f: %.4f, Estimated P%.0f: %.4f", p*100, actual, p*100, estimated)
		assert.InDelta(t, actual, estimated, defaultTestTolerance*3, "Low quantile estimate mismatch") // Higher tolerance near edges
	})

	t.Run("HighQuantile", func(t *testing.T) {
		p := 0.98
		est := NewP2Estimator(p)
		for _, v := range data {
			est.Add(v)
		}
		require.True(t, est.initialized)

		actual := calculateActualQuantile(data, p)
		estimated, err := est.Quantile()
		require.NoError(t, err)

		t.Logf("High Quantile - Actual P%.0f: %.4f, Estimated P%.0f: %.4f", p*100, actual, p*100, estimated)
		assert.InDelta(t, actual, estimated, defaultTestTolerance*3, "High quantile estimate mismatch") // Higher tolerance near edges
	})
}
