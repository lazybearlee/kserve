package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// --- Configuration Flags ---

var (
	// ... (flags for url, host, duration, totalRequests, qps, traceFile remain same) ...
	targetURL     = flag.String("url", "", "Target service ingress URL (e.g., http://localhost:8080/...) (Required)")
	hostHeader    = flag.String("host", "", "Value for the HTTP 'Host' header (e.g., my-service.my-ns.example.com) (Required)")
	duration      = flag.Duration("duration", 0, "Test duration (e.g., 30s, 5m).")
	totalRequests = flag.Uint64("total-requests", 0, "Total number of requests to send.")
	qps           = flag.Float64("qps", 0, "Queries per second for constant load.")
	traceFile     = flag.String("trace-file", "", "Path to trace file for arrival times.")

	// --- Payload Configuration ---
	instancesPerReq    = flag.Int("instances-per-req", 1, "Number of instances to include in each request payload.") // New Flag
	payloadPoolSize    = flag.Int("payload-pool-size", 100, "Number of unique base payloads to generate.")
	payloadHotSetRatio = flag.Float64("payload-hotset-ratio", 0.2, "Fraction of payload pool considered 'hot'.")
	payloadHotHitRatio = flag.Float64("payload-cache-hit-ratio", 0.8, "Probability of selecting a payload from the 'hot' set.")

	// --- Test Execution Configuration ---
	concurrency = flag.Int("concurrency", 50, "Number of concurrent worker goroutines.")
	timeout     = flag.Duration("timeout", 10*time.Second, "HTTP request timeout.")
	outputCSV   = flag.String("output-csv", "", "Path to CSV file to save raw results.")
	verbose     = flag.Bool("verbose", false, "Enable verbose logging for each request.")
	seed        = flag.Int64("seed", time.Now().UnixNano(), "Random seed.")
)

// --- Data Structures ---
type IrisInstance []float64
type RequestPayload struct {
	Instances []interface{} `json:"instances"`
}
type Result struct {
	StartTime    time.Time
	Latency      time.Duration
	StatusCode   int
	Error        error
	BodySize     int
	NumInstances int // *** ADDED: Track instances in the request ***
}

// --- Global Variables / State ---
var ( /* ... httpClient, totalSent, totalReceived, testStartTime, randSource, targetHost remain same ... */
	httpClient      *http.Client
	payloadPool     [][]byte // Stores marshalled RequestPayload JSON bytes
	hotPayloadCount int
	totalSent       uint64
	totalReceived   uint64
	testStartTime   time.Time
	randSource      *rand.Rand
	targetHost      string
)
var ( /* ... successCount, errorCount, totalLatency, statusCounts, statusCountsMu remain same ... */
	successCount          atomic.Uint64
	errorCount            atomic.Uint64
	totalLatency          atomic.Int64
	totalInstancesSuccess atomic.Uint64 // *** ADDED: Track successful instances ***
	statusCounts          map[int]uint64
	statusCountsMu        sync.Mutex
)

// --- Payload Generation ---
// Now generates payloads with specified number of instances
func generatePayloadPool(poolSize int, instancesPerPayload int) error {
	if instancesPerPayload <= 0 {
		return fmt.Errorf("instances-per-req must be positive")
	}
	payloadPool = make([][]byte, poolSize)
	minVal := 0.1
	maxVal := 8.0
	rng := rand.New(rand.NewSource(*seed))

	for i := 0; i < poolSize; i++ {
		// Generate multiple instances for the payload
		instances := make([]interface{}, instancesPerPayload)
		for instIdx := 0; instIdx < instancesPerPayload; instIdx++ {
			instanceData := make(IrisInstance, 4)
			for j := 0; j < 4; j++ {
				// Make features slightly different for each instance within payload if desired
				instanceData[j] = minVal + rng.Float64()*(maxVal-minVal) + float64(instIdx)*0.01
			}
			instances[instIdx] = instanceData
		}

		payload := RequestPayload{Instances: instances}
		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload %d: %w", i, err)
		}
		payloadPool[i] = jsonBytes
	}

	hotPayloadCount = int(math.Max(1.0, float64(poolSize)**payloadHotSetRatio))
	if hotPayloadCount >= poolSize {
		hotPayloadCount = poolSize
	}
	fmt.Printf("Generated payload pool: Size=%d, InstancesPerPayload=%d, HotSetSize=%d\n", poolSize, instancesPerPayload, hotPayloadCount)
	return nil
}

// selectPayload remains the same - it just picks a pre-marshalled payload []byte
func selectPayload() []byte { /* ... (No changes needed) ... */
	if len(payloadPool) == 0 {
		panic("Payload pool is empty!")
	}
	useHot := randSource.Float64() < *payloadHotHitRatio
	var index int
	if useHot && hotPayloadCount > 0 {
		index = randSource.Intn(hotPayloadCount)
	} else {
		index = randSource.Intn(len(payloadPool))
	}
	return payloadPool[index]
}

// --- Request Execution ---
func sendRequestWorker(ctx context.Context, id int, taskChan <-chan struct{}, resultChan chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	// if *verbose { fmt.Printf("Worker %d started\n", id) } // Reduce noise

	for {
		select {
		case <-ctx.Done():
			return // Context cancelled
		case _, ok := <-taskChan:
			if !ok {
				return
			} // Channel closed

			// --- Send Request ---
			payloadBytes := selectPayload()
			reqBody := bytes.NewReader(payloadBytes)

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, *targetURL, reqBody)
			if err != nil {
				resultChan <- Result{Error: fmt.Errorf("failed to create request: %w"), NumInstances: *instancesPerReq}
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			if targetHost != "" {
				req.Host = targetHost
			}

			start := time.Now()
			resp, err := httpClient.Do(req)
			latency := time.Since(start)

			var result Result
			result.NumInstances = *instancesPerReq // *** Store instance count ***
			result.StartTime = start
			result.Latency = latency

			if err != nil {
				result.Error = fmt.Errorf("request failed: %w", err)
			} else {
				bodySize, _ := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				result.StatusCode = resp.StatusCode
				result.BodySize = int(bodySize)
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					result.Error = fmt.Errorf("bad status: %d", resp.StatusCode)
				}
			}
			resultChan <- result
		}
	}
}

// --- Metrics Collection & Reporting ---
func collectResults(resultChan <-chan Result, doneChan chan<- struct{}, wg *sync.WaitGroup, allResults *[]Result) {
	defer wg.Done()
	statusCounts = make(map[int]uint64)

	for result := range resultChan {
		atomic.AddUint64(&totalReceived, 1)
		if result.Error != nil {
			errorCount.Add(1)
		} else {
			successCount.Add(1)
			totalLatency.Add(int64(result.Latency.Nanoseconds()))
			totalInstancesSuccess.Add(uint64(result.NumInstances)) // *** Aggregate successful instances ***
			statusCountsMu.Lock()
			statusCounts[result.StatusCode]++
			statusCountsMu.Unlock()
		}
		if allResults != nil {
			*allResults = append(*allResults, result)
		}
	}
	fmt.Println("Result collector finished.")
	close(doneChan)
}

func calculateAndReportMetrics(results []Result, elapsed time.Duration) {
	fmt.Println("\n--- Test Results ---")
	fmt.Printf("Duration:             %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total HTTP Requests:  %d\n", totalReceived)

	// --- Calculate and Print Throughputs ---
	reqThroughput := 0.0
	instThroughput := 0.0
	if elapsed.Seconds() > 0 {
		reqThroughput = float64(totalReceived) / elapsed.Seconds()
		instThroughput = float64(totalInstancesSuccess.Load()) / elapsed.Seconds()
	}
	fmt.Printf("HTTP Req Throughput:  %.2f req/s\n", reqThroughput)
	fmt.Printf("Instance Throughput:  %.2f inst/s\n", instThroughput) // *** Added Instance Throughput ***
	// ---

	successRate := 0.0
	if totalReceived > 0 {
		successRate = float64(successCount.Load()) / float64(totalReceived) * 100.0
	}
	fmt.Printf("Success Rate (HTTP):  %.2f%%\n", successRate)
	fmt.Printf("Error Count (HTTP):   %d\n", errorCount.Load())
	fmt.Printf("Total Instances Sent: %d\n", totalSent*uint64(*instancesPerReq)) // Estimated total sent
	fmt.Printf("Total Instances OK:   %d\n", totalInstancesSuccess.Load())       // Total successful instances processed

	statusCountsMu.Lock()
	if len(statusCounts) > 0 {
		fmt.Println("Status Codes:")
		codes := make([]int, 0, len(statusCounts))
		for code := range statusCounts {
			codes = append(codes, code)
		}
		sort.Ints(codes)
		for _, code := range codes {
			fmt.Printf("  %d: %d\n", code, statusCounts[code])
		}
	}
	statusCountsMu.Unlock()

	if successCount.Load() > 0 {
		if len(results) > 0 {
			latencies := make([]float64, 0, successCount.Load())
			for _, r := range results {
				if r.Error == nil {
					latencies = append(latencies, float64(r.Latency.Milliseconds()))
				}
			}
			sort.Float64s(latencies)
			if len(latencies) > 0 {
				avgLatencyMs := float64(totalLatency.Load()) / float64(successCount.Load()) / 1e6
				fmt.Printf("Latency (HTTP Requests):\n")
				fmt.Printf("  Average: %.2f ms\n", avgLatencyMs)
				fmt.Printf("  Min:     %.2f ms\n", latencies[0])
				fmt.Printf("  p50:     %.2f ms\n", percentile(latencies, 50))
				fmt.Printf("  p90:     %.2f ms\n", percentile(latencies, 90))
				fmt.Printf("  p95:     %.2f ms\n", percentile(latencies, 95))
				fmt.Printf("  p99:     %.2f ms\n", percentile(latencies, 99))
				fmt.Printf("  Max:     %.2f ms\n", latencies[len(latencies)-1])
			} else {
				fmt.Println("No successful request latencies recorded.")
			}
		} else {
			avgLatencyMs := float64(totalLatency.Load()) / float64(successCount.Load()) / 1e6
			fmt.Printf("Latency (HTTP Requests):\n")
			fmt.Printf("  Average: %.2f ms\n", avgLatencyMs)
			fmt.Println("  (Percentiles require saving raw results with -output-csv)")
		}
	} else {
		fmt.Println("No successful requests to calculate latency.")
	}
}
func percentile(data []float64, p float64) float64 { /* ... (No changes needed) ... */
	if len(data) == 0 {
		return 0.0
	}
	index := (p / 100.0) * float64(len(data)-1)
	lower := math.Floor(index)
	upper := math.Ceil(index)
	if lower == upper {
		return data[int(index)]
	}
	weight := index - lower
	return data[int(lower)]*(1.0-weight) + data[int(upper)]*weight
}
func saveResultsCSV(results []Result, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", filename, err)
	}
	defer file.Close()
	// *** ADD NumInstances to header ***
	_, err = file.WriteString("StartTimeISO,LatencyMillis,StatusCode,NumInstances,Error\n")
	if err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	for _, r := range results {
		startTimeStr := r.StartTime.Format(time.RFC3339Nano)
		latencyMs := float64(r.Latency.Milliseconds())
		statusCodeStr := strconv.Itoa(r.StatusCode)
		numInstancesStr := strconv.Itoa(r.NumInstances) // *** Get instances string ***
		errorStr := ""
		if r.Error != nil {
			errorStr = fmt.Sprintf("\"%s\"", strings.ReplaceAll(r.Error.Error(), "\"", "\"\""))
		}
		// *** ADD NumInstances to line ***
		line := fmt.Sprintf("%s,%.3f,%s,%s,%s\n", startTimeStr, latencyMs, statusCodeStr, numInstancesStr, errorStr)
		_, err = file.WriteString(line)
		if err != nil {
			return fmt.Errorf("failed to write result line to CSV: %w", err)
		}
	}
	fmt.Printf("Raw results saved to %s\n", filename)
	return nil
}

// --- Traffic Generators ---
func runConstantQPSTest(ctx context.Context, qps float64, taskChan chan<- struct{}) { /* ... (No changes needed) ... */
	if qps <= 0 {
		fmt.Println("Error: QPS must be positive for constant load test.")
		return
	}
	interval := time.Second / time.Duration(qps)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	fmt.Printf("Starting constant load test: %.2f QPS (Interval: %v)\n", qps, interval)
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Constant QPS generator stopping.")
			return
		case <-ticker.C:
			select {
			case taskChan <- struct{}{}:
				atomic.AddUint64(&totalSent, 1)
			default: /* Log saturation? */
			}
		}
	}
}
func runTraceDrivenTest(ctx context.Context, filename string, taskChan chan<- struct{}) { /* ... (No changes needed, includes burst handling) ... */
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error opening trace file %s: %v\n", filename, err)
		return
	}
	defer file.Close()
	fmt.Printf("Starting trace-driven load test using file: %s\n", filename)
	scanner := bufio.NewScanner(file)
	isFirstRequest := true
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			fmt.Println("Trace-driven generator stopping early due to context done.")
			return
		default:
		}
		line := scanner.Text()
		intervalSec, err := strconv.ParseFloat(line, 64)
		if err != nil {
			fmt.Printf("Warning: Skipping invalid line in trace file: '%s' - %v\n", line, err)
			continue
		}
		if !isFirstRequest && intervalSec > 0 {
			sleepDuration := time.Duration(intervalSec * float64(time.Second))
			sleepTimer := time.NewTimer(sleepDuration)
			select {
			case <-ctx.Done():
				fmt.Println("Trace-driven generator stopping during sleep due to context done.")
				sleepTimer.Stop()
				return
			case <-sleepTimer.C:
			}
		}
		isFirstRequest = false
		select {
		case taskChan <- struct{}{}:
			atomic.AddUint64(&totalSent, 1)
		case <-ctx.Done():
			fmt.Println("Trace-driven generator stopping before sending task.")
			return
		}
		for intervalSec == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case taskChan <- struct{}{}:
				atomic.AddUint64(&totalSent, 1)
			case <-ctx.Done():
				fmt.Println("Trace-driven generator stopping during burst send.")
				return
			}
			if !scanner.Scan() {
				break
			}
			line = scanner.Text()
			intervalSec, err = strconv.ParseFloat(line, 64)
			if err != nil {
				fmt.Printf("Warning: Skipping invalid line during burst in trace file: '%s' - %v\n", line, err)
				break
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading trace file: %v\n", err)
	}
	fmt.Println("Trace file processing finished.")
}

// --- Main Execution ---
func main() {
	flag.Parse()
	randSource = rand.New(rand.NewSource(*seed))

	// --- Validate Flags ---
	if *targetURL == "" {
		fmt.Println("Error: -url flag is required.")
		os.Exit(1)
	}
	if *hostHeader == "" { /* ... Host header check/inference ... */
		parsedURL, err := url.Parse(*targetURL)
		if err != nil || parsedURL.Host == "" {
			fmt.Printf("Error: Cannot parse URL '%s' to determine host, please provide -host flag.\n", *targetURL)
			os.Exit(1)
		}
		targetHost = parsedURL.Host
		if strings.Contains(parsedURL.Host, ":") {
			targetHost = strings.Split(parsedURL.Host, ":")[0]
			if targetHost == "localhost" || targetHost == "127.0.0.1" {
				fmt.Println("Error: Inferred host is localhost/127.0.0.1. Please provide the target service hostname using the -host flag.")
				os.Exit(1)
			}
		}
		fmt.Printf("Inferred Host header: %s (use -host flag to override)\n", targetHost)
	} else {
		targetHost = *hostHeader
	}
	// ... (rest of flag validation) ...
	if *instancesPerReq <= 0 {
		fmt.Println("Error: -instances-per-req must be positive.")
		os.Exit(1)
	} // Validate new flag
	isDurationTest := *duration > 0
	isTotalRequestsTest := *totalRequests > 0
	if (!isDurationTest && !isTotalRequestsTest) || (isDurationTest && isTotalRequestsTest) {
		fmt.Println("Error: Exactly one of -duration or -total-requests must be set.")
		os.Exit(1)
	}
	isConstantQPS := *qps > 0
	isTraceDriven := *traceFile != ""
	if (!isConstantQPS && !isTraceDriven) || (isConstantQPS && isTraceDriven) {
		fmt.Println("Error: Exactly one of -qps or -trace-file must be set.")
		os.Exit(1)
	}
	if *payloadPoolSize <= 0 {
		fmt.Println("Error: -payload-pool-size must be positive.")
		os.Exit(1)
	}
	if *concurrency <= 0 {
		fmt.Println("Error: -concurrency must be positive.")
		os.Exit(1)
	}
	if *payloadHotSetRatio < 0 || *payloadHotSetRatio > 1.0 {
		fmt.Println("Error: -payload-hotset-ratio must be between 0.0 and 1.0.")
		os.Exit(1)
	}

	// --- Initialization ---
	err := generatePayloadPool(*payloadPoolSize, *instancesPerReq)
	if err != nil {
		fmt.Printf("Error generating payload pool: %v\n", err)
		os.Exit(1)
	} // Pass instancesPerReq
	httpClient = &http.Client{Transport: &http.Transport{MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency, IdleConnTimeout: 90 * time.Second, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second}, Timeout: *timeout}
	taskChan := make(chan struct{}, *concurrency*2)
	resultChan := make(chan Result, *concurrency*10)
	collectorDoneChan := make(chan struct{})
	var allResults []Result
	if *outputCSV != "" {
		initialCap := 0
		if isTotalRequestsTest {
			initialCap = int(*totalRequests)
		}
		allResults = make([]Result, 0, initialCap)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigChan; fmt.Println("\nReceived interrupt signal, stopping..."); cancel() }()
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go collectResults(resultChan, collectorDoneChan, &collectorWg, &allResults)
	var workerWg sync.WaitGroup
	workerWg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go sendRequestWorker(ctx, i+1, taskChan, resultChan, &workerWg)
	}

	// --- Start Traffic Generator ---
	testStartTime = time.Now()
	if isConstantQPS {
		go runConstantQPSTest(ctx, *qps, taskChan)
	} else {
		go runTraceDrivenTest(ctx, *traceFile, taskChan)
	}

	// --- Wait for Test Completion ---
	if isDurationTest { /* ... (No changes needed) ... */
		fmt.Printf("Running test for duration: %v\n", *duration)
		durationTimer := time.NewTimer(*duration)
		select {
		case <-durationTimer.C:
			fmt.Println("Test duration finished.")
			cancel()
		case <-ctx.Done():
			fmt.Println("Test interrupted before duration completed.")
			durationTimer.Stop()
		}
	} else { /* ... (No changes needed) ... */
		fmt.Printf("Running test for total requests: %d\n", *totalRequests)
		checkInterval := time.Millisecond * 100
		if *qps > 0 {
			estRatePerMs := *qps / 1000.0
			if estRatePerMs > 0 {
				checkInterval = time.Duration(float64(time.Millisecond) * 100 / estRatePerMs)
				if checkInterval < 10*time.Millisecond {
					checkInterval = 10 * time.Millisecond
				}
				if checkInterval > 500*time.Millisecond {
					checkInterval = 500 * time.Millisecond
				}
			}
		}
		requestTicker := time.NewTicker(checkInterval)
		defer requestTicker.Stop()
	REQUEST_LOOP:
		for {
			select {
			case <-ctx.Done():
				fmt.Println("Test interrupted before reaching total requests.")
				break REQUEST_LOOP
			case <-requestTicker.C:
				if atomic.LoadUint64(&totalSent) >= *totalRequests {
					fmt.Println("Target number of requests sent.")
					cancel()
					break REQUEST_LOOP
				}
			}
		}
	}

	// --- Shutdown ---
	fmt.Println("Signalling traffic generator and workers to stop...")
	cancel()
	close(taskChan)
	workerWg.Wait()
	close(resultChan)
	collectorWg.Wait()
	<-collectorDoneChan
	elapsed := time.Since(testStartTime)

	// --- Report & Save ---
	calculateAndReportMetrics(allResults, elapsed)
	if *outputCSV != "" {
		err := saveResultsCSV(allResults, *outputCSV)
		if err != nil {
			fmt.Printf("Error saving results to CSV: %v\n", err)
		}
	}

	fmt.Println("Test finished.")
}
