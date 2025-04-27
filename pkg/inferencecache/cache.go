package inferencecache

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ----------------- 指标定义 -----------------

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		// Use Namespace and Subsystem for better organization if desired
		// Namespace: "kserve_agent",
		// Subsystem: "inference_cache",
		Name: "inference_cache_hits_total",
		Help: "The total number of cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_cache_misses_total",
		Help: "The total number of cache misses",
	})
	cacheEvictions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "inference_cache_evictions_total",
		Help: "The total number of cache evictions",
	})
	cacheSizeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inference_cache_size_bytes",
		Help: "Current size of the cache in bytes",
	})
	cacheEntries = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "inference_cache_entries_total",
		Help: "Total number of entries in the cache",
	})
	cacheRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "inference_cache_request_duration_seconds",
		Help:    "Duration of requests handled by the cache layer (including hits and misses/downstream)",
		Buckets: prometheus.DefBuckets, // Default buckets: .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
	})
)

// ----------------- 缓存条目 -----------------

type CacheEntry struct {
	Key          string
	Value        []byte
	LastAccess   time.Time
	TTL          time.Duration
	Size         int
	CreationTime time.Time
	StatusCode   int
	Headers      http.Header
}

// ----------------- 缓存实现 -----------------

type InferenceCache struct {
	entries         map[string]*list.Element
	lruList         *list.List
	maxSizeBytes    int // Max size in Bytes (converted from KB)
	currentSize     int
	mutex           sync.RWMutex
	cleanupTicker   *time.Ticker
	cleanupInterval time.Duration
	keyFunc         func(*http.Request) string
	stopCleanup     chan struct{}
	defaultTTL      time.Duration // Added default TTL field
}

// NewInferenceCache creates a new inference cache. maxSizeKB is DEPRECATED, use maxSizeBytes.
// Pass maxSizeMB directly if using flags.
func NewInferenceCache(maxSizeKB int, cleanupInterval time.Duration) *InferenceCache {
	// For backward compatibility if called directly, assume KB. If called from main, maxSizeKB will actually be Bytes/1024.
	// It's better to change the signature long term, but keep it for now.
	maxBytes := maxSizeKB * 1024
	if maxBytes <= 0 {
		maxBytes = 100 * 1024 * 1024 // Default 100 MiB if invalid
	}
	if cleanupInterval <= 0 {
		cleanupInterval = 5 * time.Minute
	}

	cache := &InferenceCache{
		entries:         make(map[string]*list.Element),
		lruList:         list.New(),
		maxSizeBytes:    maxBytes,
		cleanupInterval: cleanupInterval,
		stopCleanup:     make(chan struct{}),
		keyFunc:         DefaultKeyFunc,
		defaultTTL:      5 * time.Minute, // Default internal TTL
	}

	cache.startCleanup()
	cacheSizeBytes.Set(0)
	cacheEntries.Set(0)

	return cache
}

// SetDefaultTTL allows overriding the default TTL used when Cache-Control is absent.
func (c *InferenceCache) SetDefaultTTL(ttl time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if ttl > 0 {
		c.defaultTTL = ttl
	}
}

// DefaultKeyFunc is the default cache key generation function.
func DefaultKeyFunc(req *http.Request) string {
	// Read and hash request body
	var body []byte
	var err error
	// Only read body for methods that typically have one and Content-Type suggests meaningful content
	if req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodPatch {
		if req.Body != nil && req.Body != http.NoBody {
			body, err = io.ReadAll(req.Body)
			if err != nil {
				// Log error? Return empty key? Hash without body?
				// For now, proceed without body in case of error.
				body = []byte{} // Ensure body is not nil
			}
			// IMPORTANT: Replace the request body so it can be read again downstream
			req.Body = io.NopCloser(bytes.NewReader(body))
		} else {
			body = []byte{} // Ensure body is not nil
		}
	} else {
		body = []byte{} // Ensure body is not nil for other methods
	}

	// Generate hash key
	hash := sha256.New()
	hash.Write([]byte(req.Method))
	hash.Write([]byte(req.URL.Path)) // Path is usually sufficient for inference endpoint

	// Optional: Include query params if they affect the response
	// hash.Write([]byte(req.URL.RawQuery))

	// Include relevant headers that might change the response (e.g., Accept)
	// Keep this minimal to improve cache hit rate. Content-Type of *request* matters less than body hash.
	// if accept := req.Header.Get("Accept"); accept != "" { hash.Write([]byte(accept)) }

	hash.Write(body) // Hash the body content

	return hex.EncodeToString(hash.Sum(nil))
}

// SetKeyFunc sets a custom cache key generation function.
func (c *InferenceCache) SetKeyFunc(keyFunc func(*http.Request) string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.keyFunc = keyFunc
}

func (c *InferenceCache) startCleanup() {
	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go func() {
		for {
			select {
			case <-c.cleanupTicker.C:
				c.CleanupExpired()
			case <-c.stopCleanup:
				c.cleanupTicker.Stop()
				return
			}
		}
	}()
}

func (c *InferenceCache) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check if stopCleanup is already closed
	select {
	case <-c.stopCleanup:
		// Already closed
	default:
		close(c.stopCleanup)
	}
	// Clear resources (optional, GC will handle but good practice)
	c.entries = make(map[string]*list.Element)
	c.lruList.Init()
	c.currentSize = 0
	// Reset gauges explicitly
	cacheSizeBytes.Set(0)
	cacheEntries.Set(0)
}

func (c *InferenceCache) Get(key string) (*CacheEntry, bool) {
	c.mutex.RLock()
	element, found := c.entries[key]
	c.mutex.RUnlock()

	if !found {
		cacheMisses.Inc()
		return nil, false
	}

	entry := element.Value.(*CacheEntry) // Type assertion

	// Check TTL (Compare against creation time)
	if time.Since(entry.CreationTime) > entry.TTL {
		// Entry expired, remove it (write lock needed)
		c.mutex.Lock()
		// Double-check if element still exists and points to the same entry
		// in case it was removed between RUnlock and Lock.
		currentElement, stillFound := c.entries[key]
		if stillFound && currentElement == element {
			c.removeElement(element)
		}
		c.mutex.Unlock()
		cacheMisses.Inc()    // Treat expired as miss
		cacheEvictions.Inc() // Count as eviction due to TTL
		return nil, false
	}

	// Cache hit, update LRU
	c.mutex.Lock()
	entry.LastAccess = time.Now()
	c.lruList.MoveToFront(element)
	c.mutex.Unlock()

	cacheHits.Inc()
	return entry, true
}

func (c *InferenceCache) Set(key string, value []byte, statusCode int, headers http.Header, ttl time.Duration) {
	if ttl <= 0 {
		// Use the configured default TTL if provided TTL is invalid
		c.mutex.RLock()
		ttl = c.defaultTTL
		c.mutex.RUnlock()
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Estimate entry size (value + key + headers + overhead)
	headerSize := 0
	clonedHeaders := headers.Clone() // Clone headers before calculating size/storing
	for k, v := range clonedHeaders {
		headerSize += len(k)
		for _, vv := range v {
			headerSize += len(vv)
		}
	}
	// Approximate overhead for map/list entry, time fields etc.
	overhead := 256
	entrySize := len(value) + len(key) + headerSize + overhead

	// Check if entry is too large (e.g., > 50% of cache capacity)
	// Make this threshold configurable?
	if entrySize > c.maxSizeBytes/2 && c.maxSizeBytes > 0 { // Avoid division by zero if maxSizeBytes is 0
		// Log decision? "Entry too large to cache"
		return
	}
	// Reject if entry size > max size (even if cache is empty)
	if entrySize > c.maxSizeBytes && c.maxSizeBytes > 0 {
		// Log decision? "Entry larger than total cache size"
		return
	}

	// Remove existing entry if present
	if element, found := c.entries[key]; found {
		c.removeElement(element)
	}

	// Ensure capacity for the new entry
	c.ensureCapacity(entrySize) // Must be called *before* adding new entry

	// Create and add new entry
	entry := &CacheEntry{
		Key:          key,
		Value:        value, // Store original slice
		LastAccess:   time.Now(),
		TTL:          ttl,
		Size:         entrySize,
		CreationTime: time.Now(),
		StatusCode:   statusCode,
		Headers:      clonedHeaders, // Store cloned headers
	}

	element := c.lruList.PushFront(entry)
	c.entries[key] = element
	c.currentSize += entrySize

	// Update metrics
	cacheSizeBytes.Set(float64(c.currentSize))
	cacheEntries.Set(float64(len(c.entries)))
}

func (c *InferenceCache) ensureCapacity(requiredSize int) {
	// This must be called under Write Lock
	for c.currentSize+requiredSize > c.maxSizeBytes && c.lruList.Len() > 0 {
		oldest := c.lruList.Back()
		if oldest != nil {
			// Log eviction?
			// entryToEvict := oldest.Value.(*CacheEntry)
			// fmt.Printf("Evicting %s (size %d) for size %d. Current: %d/%d\n", entryToEvict.Key, entryToEvict.Size, requiredSize, c.currentSize, c.maxSizeBytes)
			c.removeElement(oldest)
			cacheEvictions.Inc()
		} else {
			break // Should not happen if Len > 0
		}
	}
}

func (c *InferenceCache) removeElement(element *list.Element) {
	// This must be called under Write Lock
	if element == nil {
		return
	} // Safety check
	entry, ok := element.Value.(*CacheEntry)
	if !ok || entry == nil {
		return
	} // Safety check

	delete(c.entries, entry.Key)
	c.lruList.Remove(element)
	c.currentSize -= entry.Size
	if c.currentSize < 0 {
		c.currentSize = 0
	} // Prevent going negative due to rounding/estimates

	// Update gauges (called under lock, safe)
	cacheSizeBytes.Set(float64(c.currentSize))
	cacheEntries.Set(float64(len(c.entries)))
}

func (c *InferenceCache) CleanupExpired() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	evictedCount := 0
	// Iterate from the back (oldest) for potentially faster cleanup if many are expired
	for e := c.lruList.Back(); e != nil; {
		prev := e.Prev() // Get previous element before potentially removing e
		entry := e.Value.(*CacheEntry)
		if now.Sub(entry.CreationTime) > entry.TTL {
			c.removeElement(e)
			evictedCount++
		}
		e = prev // Move to the next older element
	}
	if evictedCount > 0 {
		cacheEvictions.Add(float64(evictedCount)) // Update eviction counter
	}
}

// GetMetrics returns the Prometheus collectors for the cache.
func (c *InferenceCache) GetMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		cacheHits,
		cacheMisses,
		cacheEvictions,
		cacheSizeBytes,
		cacheEntries,
		cacheRequestDuration,
	}
}

// Stats returns cache statistics.
func (c *InferenceCache) Stats() map[string]interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	usage := 0.0
	if c.maxSizeBytes > 0 {
		usage = float64(c.currentSize) / float64(c.maxSizeBytes) * 100
	}
	return map[string]interface{}{
		"entries":      len(c.entries),
		"currentSize":  c.currentSize,
		"maxSizeBytes": c.maxSizeBytes,
		"usagePercent": usage,
	}
}

// ----------------- HTTP处理器 -----------------

// ResponseWriter captures response details.
type ResponseWriter struct {
	http.ResponseWriter
	StatusCode int
	Body       *bytes.Buffer // Use bytes.Buffer for easier capture
	Headers    http.Header   // Capture headers written
}

func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		StatusCode:     http.StatusOK, // Default
		Body:           new(bytes.Buffer),
		Headers:        make(http.Header),
	}
}
func (rw *ResponseWriter) Header() http.Header {
	// Return our captured headers map so we can see what was set
	return rw.Headers
}
func (rw *ResponseWriter) WriteHeader(statusCode int) {
	rw.StatusCode = statusCode
	// We don't write header to underlying writer yet, do it just before writing body or at the end
}
func (rw *ResponseWriter) Write(body []byte) (int, error) {
	// Write to our buffer first
	n, err := rw.Body.Write(body)
	if err != nil {
		return n, err
	}
	// Underlying write happens later or not at all if served from cache
	return n, nil
}

// flush writes the captured headers and body to the original ResponseWriter
func (rw *ResponseWriter) flush() (int, error) {
	// Copy captured headers to original writer
	for k, v := range rw.Headers {
		// Use Header().Set to overwrite, or Add to append? Set is safer usually.
		rw.ResponseWriter.Header()[k] = v
	}
	rw.ResponseWriter.WriteHeader(rw.StatusCode)
	if rw.Body.Len() > 0 {
		return rw.ResponseWriter.Write(rw.Body.Bytes())
	}
	return 0, nil
}

// CacheHandler is an HTTP middleware for caching.
type CacheHandler struct {
	cache   *InferenceCache
	handler http.Handler // Next handler in the chain
}

func NewCacheHandler(cache *InferenceCache, handler http.Handler) *CacheHandler {
	if cache == nil {
		// If cache is nil, just pass through? Or panic? Pass through seems safer.
		// Alternatively, return handler directly: return &CacheHandler{handler: handler} ?
		// For now, assume cache is valid. Caller should check.
		panic("NewCacheHandler: cache cannot be nil")
	}
	return &CacheHandler{
		cache:   cache,
		handler: handler,
	}
}

func (ch *CacheHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Only cache GET or potentially POST requests if deemed safe/idempotent
	// For inference, usually POST. Assume we cache POST for now. Add configurability later.
	if r.Method != http.MethodPost {
		ch.handler.ServeHTTP(w, r)
		return
	}

	start := time.Now() // Start timer for cache request duration metric

	key := ch.cache.keyFunc(r) // Generate key (consumes body and resets it)

	// --- Try Cache Get ---
	if entry, found := ch.cache.Get(key); found {
		// Cache Hit
		// Copy headers from cached entry to the response writer
		for k, v := range entry.Headers {
			w.Header()[k] = v // Use direct map access since Header() returns map
		}
		w.WriteHeader(entry.StatusCode)
		_, writeErr := w.Write(entry.Value)
		if writeErr != nil {
			// Log error writing cached response
			fmt.Fprintf(os.Stderr, "Error writing cached response: %v\n", writeErr) // Basic logging
		}
		// Observe duration for cache hit
		cacheRequestDuration.Observe(time.Since(start).Seconds())
		return
	}

	// --- Cache Miss ---
	// Create custom ResponseWriter to capture the response from the next handler
	captureWriter := NewResponseWriter(w)

	// Call the next handler (e.g., batcher or model server proxy)
	ch.handler.ServeHTTP(captureWriter, r)

	// --- Process Captured Response ---
	// Observe duration for cache miss (includes downstream processing time)
	cacheRequestDuration.Observe(time.Since(start).Seconds())

	// Only cache successful responses (2xx)
	if captureWriter.StatusCode >= 200 && captureWriter.StatusCode < 300 {
		// Determine TTL
		ttl := ch.cache.defaultTTL // Start with default
		if cacheControl := captureWriter.Headers.Get("Cache-Control"); cacheControl != "" {
			if maxAge := extractMaxAge(cacheControl); maxAge > 0 {
				ttl = time.Duration(maxAge) * time.Second
			}
		} else if expires := captureWriter.Headers.Get("Expires"); expires != "" {
			// Basic Expires header parsing (less common than Cache-Control)
			if expiresTime, err := http.ParseTime(expires); err == nil {
				if duration := time.Until(expiresTime); duration > 0 {
					ttl = duration
				}
			}
		}

		// Cache the captured response body, status, and headers
		ch.cache.Set(key, captureWriter.Body.Bytes(), captureWriter.StatusCode, captureWriter.Headers, ttl)
	}

	if _, err := captureWriter.flush(); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing captured response to client: %v\n", err) // Basic logging
	}
}

// extractMaxAge simplified helper (consider a proper library for production)
func extractMaxAge(cacheControl string) int {
	// Example: "..., max-age=60, ..."
	parts := regexp.MustCompile(`(?:^|,)\s*max-age=(\d+)\s*(?:,|$)`).FindStringSubmatch(cacheControl)
	if len(parts) > 1 {
		if age, err := strconv.Atoi(parts[1]); err == nil {
			return age
		}
	}
	return 0
}
