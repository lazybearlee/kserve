package batching

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"time"

	//"unsafe" // Not needed if using context pointer directly

	"github.com/gofrs/uuid/v5" // Using gofrs like original
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	DefaultTickInterval          = 1 * time.Millisecond
	DefaultClientResponseTimeout = 200 * time.Millisecond
)

// --- Struct Definitions (Match Original KServe + Response error handling) ---

type Request struct {
	Instances []interface{} `json:"instances"`
}

type Input struct {
	// Using pointer to context as key source
	ContextInput *context.Context // Pointer to the unique context created in ServeHTTP
	ReceivedTime time.Time
	Path         string
	RawInstances []interface{}
	ResponseChan chan Response // Use buffered channel for safety
	RequestSize  int
}

type InputInfo struct {
	ResponseChan chan Response
	Index        []int
	ReceivedTime time.Time // Can be useful for calculating queue time later
}

type Response struct {
	Message     string        `json:"message,omitempty"`
	BatchID     string        `json:"batchId,omitempty"`
	Predictions []interface{} `json:"predictions,omitempty"`
	StatusCode  int           `json:"-"` // Internal field
	Error       error         `json:"-"` // Internal field
}

type PredictionResponse struct {
	Predictions []interface{} `json:"predictions"`
}

// BatcherInfo holds state for the current batch being assembled by batch() goroutine.
type BatcherInfo struct {
	Path      string
	Instances []interface{}
	// *** Use *context.Context as key again ***
	InputMap        map[*context.Context]InputInfo
	BatchStartTime  time.Time
	LastUpdateTime  time.Time
	CurrentBatchLen int
}

// --- Global Helper Functions ---

func GetNowTime() time.Time {
	return time.Now().UTC()
}

func GenerateUUID() string {
	return uuid.Must(uuid.NewV4()).String()
}

// InitializeInfo resets the BatcherInfo. Must be called by the batch() goroutine.
func (bi *BatcherInfo) InitializeInfo() {
	bi.Path = ""
	bi.CurrentBatchLen = 0
	bi.Instances = make([]interface{}, 0, 32)
	// *** Initialize map with context pointer key ***
	bi.InputMap = make(map[*context.Context]InputInfo)
	bi.BatchStartTime = time.Time{}
	bi.LastUpdateTime = GetNowTime()
}

// --- BatchHandler Struct and Methods ---

type BatchHandler struct {
	next            http.Handler
	log             *zap.SugaredLogger
	AdaptiveBatcher *AdaptiveBatcher
	channelIn       chan Input
	batcherInfo     BatcherInfo // State ONLY accessed by batch() goroutine
	tickInterval    time.Duration
	stopChan        chan struct{}
	wg              sync.WaitGroup // Tracks main loop + worker goroutines
}

// New creates a new BatchHandler with adaptive batching.
func New(
	next http.Handler,
	logger *zap.SugaredLogger,
	adaptiveConfig AdaptiveBatcherConfig,
	channelBufferSize int,
	tickInterval time.Duration,
) *BatchHandler {
	if logger == nil {
		logger = zap.NewNop().Sugar()
	}
	if channelBufferSize <= 0 {
		channelBufferSize = 100
	}
	if tickInterval <= 0 {
		tickInterval = DefaultTickInterval
	}

	adaptiveBatcher := NewAdaptiveBatcher(adaptiveConfig, logger)

	handler := &BatchHandler{
		next:            next,
		log:             logger.Named("batch_handler"),
		AdaptiveBatcher: adaptiveBatcher,
		channelIn:       make(chan Input, channelBufferSize),
		tickInterval:    tickInterval,
		stopChan:        make(chan struct{}),
	}

	handler.log.Info("Batch handler created, starting consumption loop...")
	handler.wg.Add(1) // Add main loop to WaitGroup
	go handler.Consume()
	return handler
}

// GetMetrics returns Prometheus collectors.
func (h *BatchHandler) GetMetrics() []prometheus.Collector {
	if h.AdaptiveBatcher == nil {
		return []prometheus.Collector{}
	}
	return h.AdaptiveBatcher.GetMetrics()
}

// Stop gracefully shuts down the batcher.
func (h *BatchHandler) Stop() {
	h.log.Info("Stopping batch handler...")
	close(h.stopChan)
	h.wg.Wait() // Wait for main loop AND workers
	h.log.Info("Batch handler stopped.")
}

// processAndRespond handles a single formed batch in its own goroutine.
// Operates on COPIED data.
func (h *BatchHandler) processAndRespond(
	batchID string,
	path string,
	instances []interface{},
	// *** Use *context.Context as key ***
	inputMap map[*context.Context]InputInfo,
	batchStartTime time.Time) {

	h.wg.Add(1) // Add worker to WaitGroup
	defer h.wg.Done()

	startTime := GetNowTime()
	currentBatchSize := len(instances)
	h.log.Debugw("Processing batch goroutine started", "batchId", batchID, "size", currentBatchSize, "path", path)

	// Panic recovery for safety in goroutine
	defer func() {
		if r := recover(); r != nil {
			h.log.Errorw("Panic recovered during batch processing", "batchId", batchID, "panic", r)
			err := fmt.Errorf("batch processing panic: %v", r)
			h.distributeErrorResponse(inputMap, http.StatusInternalServerError, err.Error(), err)
		}
	}()

	// --- Prepare and Send Downstream Request ---
	jsonStr, err := json.Marshal(Request{Instances: instances})
	if err != nil {
		h.log.Errorw("Failed to marshal batch request", "batchId", batchID, zap.Error(err))
		h.distributeErrorResponse(inputMap, http.StatusInternalServerError, "failed to marshal batch request", err)
		return
	}
	reader := bytes.NewReader(jsonStr)
	req := httptest.NewRequest(http.MethodPost, path, reader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KServe-Batch-Id", batchID)
	rr := httptest.NewRecorder()

	h.next.ServeHTTP(rr, req) // *** Call downstream ***

	// --- Process Downstream Response ---
	latency := GetNowTime().Sub(startTime)
	responseBody := rr.Body.Bytes()
	statusCode := rr.Code

	// --- Observe Batch ---
	totalBatchLatency := GetNowTime().Sub(batchStartTime)
	h.AdaptiveBatcher.ObserveBatch(currentBatchSize, totalBatchLatency)

	// --- Distribute Response ---
	if statusCode != http.StatusOK {
		limitedBody := string(responseBody)
		if len(limitedBody) > 256 {
			limitedBody = limitedBody[:256] + "..."
		}
		h.log.Warnw("Downstream service returned error", "batchId", batchID, "statusCode", statusCode, "path", path, "responseBodyExcerpt", limitedBody, "downstreamLatency", latency)
		h.distributeErrorResponse(inputMap, statusCode, fmt.Sprintf("Downstream error (%d)", statusCode), errors.New("downstream service error"))
	} else {
		var predResp PredictionResponse
		err := json.Unmarshal(responseBody, &predResp)
		if err != nil {
			h.log.Errorw("Failed to unmarshal downstream response", "batchId", batchID, "path", path, "downstreamLatency", latency, zap.Error(err))
			h.distributeErrorResponse(inputMap, http.StatusInternalServerError, "failed to parse downstream response", err)
		} else if len(predResp.Predictions) != currentBatchSize {
			errMsg := "prediction size mismatch"
			h.log.Errorw(errMsg, "batchId", batchID, "path", path, "expectedSize", currentBatchSize, "actualSize", len(predResp.Predictions), "downstreamLatency", latency)
			h.distributeErrorResponse(inputMap, http.StatusInternalServerError, errMsg, errors.New(errMsg))
		} else {
			h.log.Infow("Successfully processed batch", "batchId", batchID, "path", path, "batchSize", currentBatchSize, "totalBatchLatency", totalBatchLatency, "downstreamLatency", latency)
			h.distributeSuccessResponse(inputMap, batchID, predResp.Predictions)
		}
	}
	h.log.Debugw("Processing batch goroutine finished", "batchId", batchID)
}

// distributeSuccessResponse sends successful predictions back.
// *** Use *context.Context as key ***
func (h *BatchHandler) distributeSuccessResponse(inputMap map[*context.Context]InputInfo, batchID string, allPredictions []interface{}) {
	h.log.Debugw("Distributing success responses", "batchId", batchID, "numRequests", len(inputMap))

	for ctxPtr, info := range inputMap { // Use ctxPtr as key variable name
		predictions := make([]interface{}, len(info.Index))
		for i, batchIndex := range info.Index {
			if batchIndex >= 0 && batchIndex < len(allPredictions) {
				predictions[i] = allPredictions[batchIndex]
			} else {
				h.log.Errorw("Prediction index out of bounds", "batchId", batchID, "ctxPtr", fmt.Sprintf("%p", ctxPtr), "index", batchIndex, "totalPredictions", len(allPredictions))
				predictions[i] = map[string]string{"error": "internal prediction index error"}
			}
		}
		res := Response{BatchID: batchID, Predictions: predictions, StatusCode: http.StatusOK}

		// Send response (non-blocking preferable)
		select {
		case info.ResponseChan <- res:
			// h.log.Debugw("Successfully sent response", "batchId", batchID, "ctxPtr", fmt.Sprintf("%p", ctxPtr))
		case <-time.After(DefaultClientResponseTimeout):
			h.log.Warnw("Timeout sending success response to client channel", "batchId", batchID, "ctxPtr", fmt.Sprintf("%p", ctxPtr))
		}
		// Close channel after sending
		if info.ResponseChan != nil {
			close(info.ResponseChan)
		}
	}
}

// distributeErrorResponse sends error responses back.
// *** Use *context.Context as key ***
func (h *BatchHandler) distributeErrorResponse(inputMap map[*context.Context]InputInfo, statusCode int, message string, err error) {
	h.log.Debugw("Distributing error response", "statusCode", statusCode, "message", message, "numRequests", len(inputMap), zap.Error(err))

	for ctxPtr, info := range inputMap { // Use ctxPtr as key variable name
		res := Response{StatusCode: statusCode, Message: message, Error: err}
		// Send response (non-blocking preferable)
		select {
		case info.ResponseChan <- res:
			// h.log.Debugw("Successfully sent error response", "ctxPtr", fmt.Sprintf("%p", ctxPtr))
		case <-time.After(DefaultClientResponseTimeout):
			h.log.Warnw("Timeout sending error response to client channel", "ctxPtr", fmt.Sprintf("%p", ctxPtr), "statusCode", statusCode)
		}
		if info.ResponseChan != nil {
			close(info.ResponseChan)
		}
	}
}

// batch is the main single goroutine that collects requests and triggers batch processing.
func (h *BatchHandler) batch() {
	defer h.wg.Done() // Signal main loop finished
	h.log.Info("Batching loop started.")

	ticker := time.NewTicker(h.tickInterval)
	defer ticker.Stop()

	// Initialize batcherInfo (only accessed by this goroutine)
	h.batcherInfo.InitializeInfo()

	for {
		// Get dynamic parameters
		maxBatchSize, maxLatency := h.AdaptiveBatcher.GetBatchingParams()
		// Observe queue length
		h.AdaptiveBatcher.ObserveQueueLength(h.batcherInfo.CurrentBatchLen)

		// --- Check Trigger Conditions (No lock needed) ---
		trigger := false
		if h.batcherInfo.CurrentBatchLen > 0 {
			if h.batcherInfo.CurrentBatchLen >= maxBatchSize {
				trigger = true
				h.log.Debugw("Batch triggered by size", "currentSize", h.batcherInfo.CurrentBatchLen, "maxSize", maxBatchSize)
			} else if !h.batcherInfo.BatchStartTime.IsZero() {
				elapsedTime := GetNowTime().Sub(h.batcherInfo.BatchStartTime)
				if elapsedTime >= maxLatency {
					trigger = true
					h.log.Debugw("Batch triggered by latency", "currentSize", h.batcherInfo.CurrentBatchLen, "elapsed", elapsedTime, "maxLatency", maxLatency)
				}
			}
		}

		// --- Trigger Batch Processing ---
		if trigger {
			// Copy data needed for processing goroutine
			batchID := GenerateUUID()
			path := h.batcherInfo.Path
			instancesToProcess := make([]interface{}, h.batcherInfo.CurrentBatchLen)
			copy(instancesToProcess, h.batcherInfo.Instances)
			// *** Use *context.Context as key ***
			inputMapCopy := make(map[*context.Context]InputInfo, len(h.batcherInfo.InputMap))
			for k, v := range h.batcherInfo.InputMap {
				inputMapCopy[k] = v
			}
			batchStartTime := h.batcherInfo.BatchStartTime

			// Reset batcherInfo immediately
			h.batcherInfo.InitializeInfo()

			// Launch processing in a new goroutine
			h.log.Debugw("Launching batch processing goroutine", "batchId", batchID, "size", len(instancesToProcess))
			go h.processAndRespond(batchID, path, instancesToProcess, inputMapCopy, batchStartTime)

			continue // Skip select wait, check for new input immediately
		}

		// --- Wait for Events ---
		select {
		case <-h.stopChan:
			h.log.Info("Stop signal received, cleaning up batch loop...")
			if h.batcherInfo.CurrentBatchLen > 0 {
				finalSize := h.batcherInfo.CurrentBatchLen
				h.log.Infow("Processing final batch before shutdown", "size", finalSize)
				batchID := GenerateUUID()
				path := h.batcherInfo.Path
				instancesToProcess := make([]interface{}, finalSize)
				copy(instancesToProcess, h.batcherInfo.Instances)
				// *** Use *context.Context as key ***
				inputMapCopy := make(map[*context.Context]InputInfo, len(h.batcherInfo.InputMap))
				for k, v := range h.batcherInfo.InputMap {
					inputMapCopy[k] = v
				}
				batchStartTime := h.batcherInfo.BatchStartTime
				// Launch final batch processing
				go h.processAndRespond(batchID, path, instancesToProcess, inputMapCopy, batchStartTime)
			}
			h.log.Info("Batching loop finished.")
			return

		case req, ok := <-h.channelIn:
			if !ok {
				h.log.Warn("Input channel closed unexpectedly. Exiting batch loop.")
				return
			}

			// --- Add Request to Current Batch (No lock needed) ---
			now := GetNowTime()
			h.batcherInfo.LastUpdateTime = now

			if h.batcherInfo.CurrentBatchLen == 0 {
				h.batcherInfo.BatchStartTime = now
				h.batcherInfo.Path = req.Path
			} else if h.batcherInfo.Path != req.Path {
				h.log.Errorw("Request path mismatch in batch", "existingPath", h.batcherInfo.Path, "newPath", req.Path, "ctxPtr", fmt.Sprintf("%p", req.ContextInput))
				res := Response{StatusCode: http.StatusBadRequest, Message: "Path mismatch in batch", Error: errors.New("path mismatch")}
				select {
				case req.ResponseChan <- res:
				default:
				}
				close(req.ResponseChan)
				continue
			}

			startIdx := h.batcherInfo.CurrentBatchLen
			h.batcherInfo.Instances = append(h.batcherInfo.Instances, req.RawInstances...)
			h.batcherInfo.CurrentBatchLen = len(h.batcherInfo.Instances)
			indices := make([]int, req.RequestSize)
			for i := 0; i < req.RequestSize; i++ {
				indices[i] = startIdx + i
			}

			// *** Use Context Pointer as key ***
			h.log.Debugw("Adding request to batch map", "ctxPtr", fmt.Sprintf("%p", req.ContextInput), "numInstances", req.RequestSize)
			h.batcherInfo.InputMap[req.ContextInput] = InputInfo{
				ResponseChan: req.ResponseChan,
				Index:        indices,
				ReceivedTime: req.ReceivedTime,
			}

		case <-ticker.C:
			// Loop will re-evaluate trigger conditions
			continue
		}
	}
}

// Consume starts the main batch processing loop.
func (h *BatchHandler) Consume() {
	h.batch()
}

// ServeHTTP handles incoming HTTP requests.
func (h *BatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	predictVerb := regexp.MustCompile(`:predict$`)
	if r.Method != http.MethodPost || !predictVerb.MatchString(r.URL.Path) {
		h.next.ServeHTTP(w, r)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.log.Warnw("Failed to read request body", zap.Error(err))
		http.Error(w, "Cannot read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var req Request
	if err = json.Unmarshal(body, &req); err != nil {
		h.log.Warnw("Failed to unmarshal request body", "body", string(body), zap.Error(err))
		http.Error(w, "Cannot unmarshal request body", http.StatusBadRequest)
		return
	}
	if len(req.Instances) == 0 {
		h.log.Warn("Received request with no instances")
		http.Error(w, "Request must contain instances", http.StatusBadRequest)
		return
	}

	// --- Prepare Input for Batcher ---
	receivedTime := GetNowTime()
	responseChan := make(chan Response, 1) // Buffered channel
	// *** Create NEW context for EACH request to get a unique pointer key ***
	ctx := context.Background() // This makes &ctx unique per call

	input := Input{
		// *** Pass pointer to the new context ***
		ContextInput: &ctx,
		ReceivedTime: receivedTime,
		Path:         r.URL.Path,
		RawInstances: req.Instances,
		ResponseChan: responseChan,
		RequestSize:  len(req.Instances),
	}
	h.log.Debugw("Received request, queuing", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, "instances", len(req.Instances))

	// --- Send to Batcher Channel ---
	select {
	case h.channelIn <- input:
		// Successfully queued
	case <-r.Context().Done():
		h.log.Warnw("Client closed connection before request queuing", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, zap.Error(r.Context().Err()))
		http.Error(w, "Client closed connection", 499)
		close(responseChan)
		return
	case <-time.After(1 * time.Second):
		h.log.Errorw("Timeout queuing request (channel full or blocked)", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, "channelSize", cap(h.channelIn))
		http.Error(w, "Server busy, please retry", http.StatusServiceUnavailable)
		close(responseChan)
		return
	}

	// --- Wait for Response from Batcher ---
	var response Response
	var ok bool
	// h.log.Debugw("Waiting for response from batcher", "ctxPtr", fmt.Sprintf("%p", &ctx))
	select {
	case response, ok = <-responseChan:
		if !ok {
			h.log.Errorw("Response channel closed unexpectedly", "ctxPtr", fmt.Sprintf("%p", &ctx))
			http.Error(w, "Internal server error processing request", http.StatusInternalServerError)
			return
		}
		// h.log.Debugw("Received response from batcher", "ctxPtr", fmt.Sprintf("%p", &ctx), "statusCode", response.StatusCode)
	case <-r.Context().Done():
		h.log.Warnw("Client closed connection while waiting for batch response", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, zap.Error(r.Context().Err()))
		return
	}

	// --- Send Response to Client ---
	if response.Error != nil {
		statusCode := response.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		clientErrMsg := response.Message
		if clientErrMsg == "" {
			clientErrMsg = "Internal server error during batch processing"
		}
		http.Error(w, clientErrMsg, statusCode)
		return
	}
	rspBytes, err := json.Marshal(response)
	if err != nil {
		h.log.Errorw("Failed to marshal final response", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, zap.Error(err))
		http.Error(w, "Internal server error marshalling response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(rspBytes)
	if err != nil {
		h.log.Warnw("Failed to write response to client", "ctxPtr", fmt.Sprintf("%p", &ctx), "path", r.URL.Path, zap.Error(err))
	}
}
