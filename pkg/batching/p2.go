package batching

import (
	"fmt"
	"math"
	"sort"
)

const numMarkers = 5 // P² uses 5 markers

// P2Estimator implements the P² algorithm based on Jain/Chlamtac paper
// and common C++ reference implementations.
type P2Estimator struct {
	p           float64             // Target quantile (0 < p < 1)
	q           [numMarkers]float64 // Marker heights (q_0..q_4)
	n           [numMarkers]int     // Actual integer positions (n_0..n_4)
	dn          [numMarkers]float64 // Desired fractional positions (n'_0..n'_4)
	np          [numMarkers]float64 // Desired position increments (p_i)
	count       int                 // Total observations processed
	initialized bool
}

// NewP2Estimator creates a new estimator for the given quantile.
func NewP2Estimator(quantile float64) *P2Estimator {
	if quantile <= 0 || quantile >= 1 {
		panic("P2Estimator: Quantile must be between 0 and 1")
	}
	est := &P2Estimator{p: quantile}
	// Precompute desired increments (p_i = [0, p/2, p, (1+p)/2, 1])
	est.np[0] = 0.0
	est.np[1] = quantile / 2.0
	est.np[2] = quantile
	est.np[3] = (1.0 + quantile) / 2.0
	est.np[4] = 1.0
	return est
}

// Add processes a new observation.
func (est *P2Estimator) Add(value float64) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return // Ignore non-finite values
	}

	// --- Initialization Phase ---
	if !est.initialized {
		if est.count < numMarkers {
			est.q[est.count] = value // Store initial values
			est.count++
			if est.count == numMarkers {
				// Sort initial marker heights
				sort.Float64s(est.q[:])

				// Check for NaN/Inf after sort (safety)
				for _, m := range est.q {
					if math.IsNaN(m) || math.IsInf(m, 0) {
						panic(fmt.Sprintf("P2Estimator: Non-finite marker during init: %v", est.q))
					}
				}

				// Initialize actual positions
				for i := 0; i < numMarkers; i++ {
					est.n[i] = i + 1 // n = [1, 2, 3, 4, 5]
				}

				// Initialize desired positions based on n=5 and increments np
				// dn[i] = 1 + np[i] * (n - 1) where n=5
				n_init := float64(numMarkers)
				for i := 0; i < numMarkers; i++ {
					est.dn[i] = 1.0 + est.np[i]*(n_init-1.0)
				}

				est.initialized = true
				// fmt.Println("P2 Initialized. q:", est.q, "n:", est.n, "dn:", est.dn, "np:", est.np)
			}
		}
		return // Return after handling initialization sample
	}

	// --- Processing Phase (After Initialization) ---
	est.count++ // Increment total observation count

	// Determine cell k and update extreme markers (q[0], q[4]) if necessary
	k := 0 // Default assumption: value >= q[4]
	if value < est.q[0] {
		est.q[0] = value
		k = 0
	} else if value < est.q[1] {
		k = 0
	} else if value < est.q[2] {
		k = 1
	} else if value < est.q[3] {
		k = 2
	} else if value < est.q[4] {
		k = 3
	} else { // value >= est.q[4]
		// Update max marker only if strictly greater
		if value > est.q[4] {
			est.q[4] = value
		}
		k = 3 // Conceptually falls in cell 3 or beyond
	}

	// Increment actual positions n_i for markers > k
	for i := k + 1; i < numMarkers; i++ {
		est.n[i]++
	}

	// Update desired positions n'_i by adding precalculated increments p_i
	for i := 0; i < numMarkers; i++ {
		est.dn[i] += est.np[i]
	}

	// Adjust heights of internal markers q_1, q_2, q_3
	for i := 1; i <= 3; i++ {
		est.adjustMarker(i)
	}
	// fmt.Println("After Add:", value, "Count:", est.count, "q:", est.q, "n:", est.n, "dn:", est.dn)
}

// adjustMarker adjusts the height and potentially the actual position of marker i.
func (est *P2Estimator) adjustMarker(i int) {
	if i <= 0 || i >= numMarkers-1 {
		return // Cannot adjust endpoints q0, q4
	}

	// Calculate deviation d = n'_i - n_i
	d_float := est.dn[i] - float64(est.n[i])

	// Check adjustment conditions (deviation >= 1 or <= -1) AND space between neighbours
	// Note: Paper uses d <= -1 and n(i-1) - n(i) < -1, which is n(i) - n(i-1) > 1
	space_right := est.n[i+1] - est.n[i]
	space_left := est.n[i] - est.n[i-1]

	need_adjust := false
	d_int := 0 // Adjustment direction (+1 or -1)

	if d_float >= 1.0 && space_right > 1 {
		need_adjust = true
		d_int = 1
	} else if d_float <= -1.0 && space_left > 1 {
		need_adjust = true
		d_int = -1
	}

	if need_adjust {
		// Calculate the new height using interpolation
		new_qi := est.calculateInterpolatedHeight(i, d_int)

		// Validate and clamp the result *strictly* between neighbours
		q_prev := est.q[i-1]
		q_next := est.q[i+1]

		if !math.IsNaN(new_qi) && !math.IsInf(new_qi, 0) {
			// Clamp strictly between neighbours
			if new_qi <= q_prev {
				new_qi = math.Nextafter(q_prev, q_next)
			}
			if new_qi >= q_next {
				new_qi = math.Nextafter(q_next, q_prev)
			}

			// Check again after clamping
			if new_qi > q_prev && new_qi < q_next {
				est.q[i] = new_qi
				// *** Crucial: Update the actual position n[i] according to adjustment direction ***
				est.n[i] += d_int
			} else {
				// fmt.Printf("P2 Debug: Marker %d adjustment skipped, clamped value %.4f not strictly between (%.4f, %.4f)\n", i, new_qi, q_prev, q_next)
			}
		} // else: interpolation failed, do nothing
	}
}

// calculateInterpolatedHeight performs parabolic or linear interpolation for marker i.
// d_int is +1 or -1, indicating the direction of adjustment.
func (est *P2Estimator) calculateInterpolatedHeight(i int, d_int int) float64 {
	q_prev := est.q[i-1]
	q_curr := est.q[i]
	q_next := est.q[i+1]
	n_prev := float64(est.n[i-1])
	n_curr := float64(est.n[i])
	n_next := float64(est.n[i+1])
	d_float := float64(d_int) // Use direction (+1 or -1) in formula

	// --- Denominator Checks for safety ---
	dn_next_curr := n_next - n_curr
	dn_curr_prev := n_curr - n_prev
	dn_next_prev := n_next - n_prev
	if dn_next_curr <= 0 || dn_curr_prev <= 0 || dn_next_prev <= 0 {
		// fmt.Printf("Interpolate Error: Non-positive pos diff. i=%d, n=[%.0f, %.0f, %.0f]\n", i, n_prev, n_curr, n_next)
		return q_curr // Cannot interpolate, return current value
	}

	// Try parabolic interpolation formula from paper (using d = +/- 1)
	// q_i' = q_i + d/(n_{i+1}-n_{i-1}) * [ (n_i - n_{i-1} + d)(q_{i+1}-q_i)/(n_{i+1}-n_i) + (n_{i+1}-n_i-d)(q_i-q_{i-1})/(n_i-n_{i-1}) ]
	term1 := d_float / dn_next_prev
	term2_num := (n_curr - n_prev + d_float) * (q_next - q_curr)
	term3_num := (n_next - n_curr - d_float) * (q_curr - q_prev)

	// Avoid division by zero if heights are equal
	term2 := 0.0
	if dn_next_curr != 0 {
		term2 = term2_num / dn_next_curr
	}
	term3 := 0.0
	if dn_curr_prev != 0 {
		term3 = term3_num / dn_curr_prev
	}

	parabolic_qi := q_curr + term1*(term2+term3)

	// Use parabolic if it's valid and strictly between neighbors
	if !math.IsNaN(parabolic_qi) && !math.IsInf(parabolic_qi, 0) && parabolic_qi > q_prev && parabolic_qi < q_next {
		return parabolic_qi
	}

	// Fallback to linear interpolation using d_int = +/- 1
	// q_i' = q_i + d * (q_{i+d} - q_i) / (n_{i+d} - n_i)
	linear_qi := q_curr // Default to no change if linear fails
	if d_int > 0 {
		// Interpolate between q_i and q_{i+1}
		if dn_next_curr != 0 { // Avoid division by zero
			linear_qi = q_curr + (q_next-q_curr)*(d_float)/dn_next_curr
		}
	} else { // d_int < 0
		// Interpolate between q_{i-1} and q_i
		if dn_curr_prev != 0 { // Avoid division by zero
			linear_qi = q_curr + (q_curr-q_prev)*(d_float)/dn_curr_prev // d_float is negative
		}
	}

	if math.IsNaN(linear_qi) || math.IsInf(linear_qi, 0) {
		// fmt.Printf("Interpolate Error: Linear resulted in non-finite. i=%d, q=[%.4f, %.4f, %.4f], d_int=%d\n", i, q_prev, q_curr, q_next, d_int)
		return q_curr // Fallback if linear also fails
	}

	return linear_qi
}

// Quantile returns the current estimated quantile value (marker q_2).
func (est *P2Estimator) Quantile() (float64, error) {
	if !est.initialized {
		return 0.0, fmt.Errorf("P2Estimator: not enough observations (need %d, have %d)", numMarkers, est.count)
	}
	// Quantile estimate is the height of the middle marker q_2
	quantileValue := est.q[2]

	if math.IsNaN(quantileValue) || math.IsInf(quantileValue, 0) {
		return 0.0, fmt.Errorf("P2Estimator: calculated quantile is non-finite (NaN/Inf)")
	}
	return quantileValue, nil
}
