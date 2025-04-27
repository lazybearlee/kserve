/*
Copyright 2023 The KServe Authors.
Modifications copyright 2024 [Your Name/Org].
*/

package main

import (
	"context"
	//"encoding/json" // No longer needed for spec file
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/zapr"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag" // Using pflag for better flag parsing
	"go.uber.org/zap"
	"knative.dev/networking/pkg/http/header"
	proxy "knative.dev/networking/pkg/http/proxy"
	pkglogging "knative.dev/pkg/logging"
	pkgnet "knative.dev/pkg/network"
	pkghandler "knative.dev/pkg/network/handlers"
	"knative.dev/pkg/signals"
	"knative.dev/serving/pkg/queue"
	"knative.dev/serving/pkg/queue/health"
	"knative.dev/serving/pkg/queue/readiness"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kserve/kserve/pkg/agent"
	"github.com/kserve/kserve/pkg/agent/storage"
	"github.com/kserve/kserve/pkg/apis/serving/v1beta1" // Still needed for LogMode enum
	"github.com/kserve/kserve/pkg/batcher"
	"github.com/kserve/kserve/pkg/batching"
	"github.com/kserve/kserve/pkg/inferencecache"
	kfslogger "github.com/kserve/kserve/pkg/logger"
)

// --- Agent Flags ---
// Define flags corresponding to arguments injected by the webhook
// Default values act as fallbacks if annotation/injection is missing

var (
	// Agent/Server Flags
	port          = flag.String("port", "9081", "Agent port")
	componentPort = flag.Int("component-port", 8080, "Component port (user container)")
	// componentSpecPath Removed - Configuration comes via flags now
	// componentType Removed - Not needed without spec file

	// Model Puller Flags (Injected via args if enabled)
	enablePuller = flag.Bool("enable-puller", false, "Enable model puller")
	configDir    = flag.String("config-dir", "/mnt/configs", "Directory for model config files (watcher)")
	modelDir     = flag.String("model-dir", "/mnt/models", "Directory for model files (downloader)")

	// Logger Flags (Injected via args if enabled)
	enableLogger     = flag.Bool("enable-logger", false, "Enable request/response logging (determined by webhook)") // Added flag to know if logger args were injected
	logUrl           = flag.String("log-url", "", "The URL to send request/response logs to")
	workers          = flag.Int("workers", 5, "Number of workers for log dispatcher")
	sourceUri        = flag.String("source-uri", "", "The source URI to use when publishing cloudevents")
	logMode          = flag.String("log-mode", string(v1beta1.LogAll), "Log mode ('all', 'request', 'response')")
	inferenceService = flag.String("inference-service", "", "InferenceService name header")
	namespace        = flag.String("namespace", "", "Namespace header")
	endpoint         = flag.String("endpoint", "", "Endpoint name header")
	component        = flag.String("component", "", "Component name header")
	metadataHeaders  = flag.StringSlice("metadata-headers", nil, "Allowed metadata headers")
	CaCertFile       = flag.String("logger-ca-cert-file", "/etc/kserve-agent/logger/service-ca.crt", "Logger CA certificate file mount path") // Default mount path
	TlsSkipVerify    = flag.Bool("logger-tls-skip-verify", false, "Skip verification of TLS certificate")

	// Batcher Flags (Injected via args if enabled)
	enableBatcher = flag.Bool("enable-batcher", false, "Enable request batcher (determined by webhook)") // Flag to know if batcher args were injected
	maxBatchSize  = flag.Int("max-batchsize", 32, "Max Batch Size (used by static, or as Max/Initial for adaptive)")
	maxLatency    = flag.Int("max-latency", 5000, "Max Latency in ms (used by static, or as Max/Initial for adaptive)")
	// Adaptive specific flags (also injected by webhook if adaptive is enabled in CRD)
	enableAdaptiveBatcher   = flag.Bool("enable-adaptive-batcher", false, "Explicitly enable adaptive batching strategy") // Replaces 'UseDynamicBatchSize' ? Let's use this more descriptive flag name.
	minBatchSize            = flag.Int("min-batchsize", 1, "Min Batch Size for adaptive batcher")
	minLatency              = flag.Int("min-latency", 1, "Min Latency (ms) for adaptive batcher")
	targetLatency           = flag.Int("target-latency", 0, "Target Latency (ms) for adaptive batcher (REQUIRED if adaptive enabled)") // Default 0 to force check
	targetLatencyPercentile = flag.String("target-latency-percentile", "0.95", "Target percentile (string, e.g., \"0.95\") for adaptive")
	queueLengthThreshold    = flag.String("queue-length-threshold", "0.7", "Queue length threshold (string, e.g., \"0.7\") for adaptive")
	stateTransitionCooldown = flag.Int("state-transition-cooldown", 5000, "State transition cooldown (ms) for adaptive batcher")
	// Agent-wide Adaptive Defaults (NOT injected, configured on agent deployment)
	adaptiveDefaultBatchSizeIncrStep   = flag.Int("adaptive-agent-bs-incr-step", 1, "Agent default: adaptive batch size increase step")
	adaptiveDefaultLatencyIncrStepMs   = flag.Int("adaptive-agent-lat-incr-step-ms", 1, "Agent default: adaptive latency increase step (ms)")
	adaptiveDefaultWarnBSDecrFactor    = flag.Float64("adaptive-agent-warn-bs-decr", 0.85, "Agent default: BS decrease factor (Warning)")
	adaptiveDefaultWarnLatDecrFactor   = flag.Float64("adaptive-agent-warn-lat-decr", 0.90, "Agent default: Lat decrease factor (Warning)")
	adaptiveDefaultDangerBSDecrFactor  = flag.Float64("adaptive-agent-danger-bs-decr", 0.70, "Agent default: BS decrease factor (Danger)")
	adaptiveDefaultDangerLatDecrFactor = flag.Float64("adaptive-agent-danger-lat-decr", 0.75, "Agent default: Lat decrease factor (Danger)")

	// Cache Flags (Injected via args if enabled)
	enableCache            = flag.Bool("enable-cache", false, "Enable inference response cache (determined by webhook)") // Flag to know if cache args were injected
	cacheMaxSizeMb         = flag.Int("cache-max-size-mb", 100, "Max cache size in MiB")
	cacheDefaultTtlSeconds = flag.Int("cache-default-ttl-seconds", 300, "Default cache TTL in seconds")
	cacheCleanupInterval   = flag.Duration("cache-cleanup-interval", 5*time.Minute, "Interval for cleaning up expired cache items")

	// Probing Flags (From Knative Env Vars primarily)
	// readinessProbeTimeout Removed flag - use Knative env

	// Unix Socket (remain the same)
	unixSocketPath = "@/kserve/agent.sock"

	// Prometheus Metrics Port
	metricsPort = flag.Int("metrics-port", 9082, "Port for exposing Prometheus metrics")
)

const (
	drainSleepDuration = 30 * time.Second
)

// config struct for Knative environment variables (remains the same)
type config struct {
	ContainerConcurrency         int    `split_words:"true"`
	QueueServingPort             int    `split_words:"true"`
	UserPort                     int    `split_words:"true"`
	RevisionTimeoutSeconds       int    `split_words:"true"`
	ServingReadinessProbe        string `split_words:"true" required:"false"` // Make optional as agent might run without QP
	EnableHTTP2AutoDetection     bool   `envconfig:"ENABLE_HTTP2_AUTO_DETECTION"`
	EnableMultiContainerProbes   bool   `split_words:"true"`
	ServingLoggingConfig         string `split_words:"true"`
	ServingLoggingLevel          string `split_words:"true"`
	ServingRequestLogTemplate    string `split_words:"true"`
	ServingEnableRequestLog      bool   `split_words:"true"`
	ServingEnableProbeRequestLog bool   `split_words:"true"`
}

// loggerArgs struct (remains the same)
type loggerArgs struct {
	enabled          bool
	loggerType       v1beta1.LoggerType
	logUrl           *url.URL
	sourceUrl        *url.URL
	inferenceService string
	namespace        string
	endpoint         string
	component        string
	metadataHeaders  []string
	certName         string
	tlsSkipVerify    bool
}

// cacheArgs struct (remains the same)
type cacheArgs struct {
	enabled           bool
	maxSizeBytes      int
	defaultTTLSeconds int
}

// batcherConfigHolder struct (remains the same)
type batcherConfigHolder struct {
	enabled bool
	config  interface{} // staticBatcherArgs or batcher.AdaptiveBatcherConfig
}
type staticBatcherArgs struct {
	maxBatchSize int
	maxLatency   int
}

// loadAndExtractComponentSpec Removed - config comes via flags

func main() {
	flag.Parse()

	var knativeEnv config
	if err := envconfig.Process("", &knativeEnv); err != nil {
		fmt.Fprintln(os.Stderr, "Error processing Knative env vars:", err)
		os.Exit(1)
	}
	logCfg := knativeEnv.ServingLoggingConfig
	if logCfg == "" {
		logCfg = "{}"
	}
	logger, _ := pkglogging.NewLogger(logCfg, knativeEnv.ServingLoggingLevel)
	defer logger.Sync()
	ctrl.SetLogger(zapr.NewLogger(logger.Desugar()))

	logger.Info("Initializing KServe Agent...")
	// Log important flag values
	logger.Infof("Agent Flags: port=%s, componentPort=%d, enablePuller=%t, enableBatcher=%t, enableAdaptive=%t, enableCache=%t, enableLogger=%t",
		*port, *componentPort, *enablePuller, *enableBatcher, *enableAdaptiveBatcher, *enableCache, *enableLogger)

	// --- Setup Readiness Probe ---
	probe := func() bool { return true }
	if knativeEnv.ServingReadinessProbe != "" {
		probe = buildProbe(logger, knativeEnv.ServingReadinessProbe, knativeEnv.EnableHTTP2AutoDetection, knativeEnv.EnableMultiContainerProbes).ProbeContainer
	} else {
		logger.Info("Using default readiness probe.")
	}
	// --- Start Model Puller (Optional) ---
	if *enablePuller {
		startModelPuller(logger)
	} else {
		logger.Info("Model puller disabled.")
	}
	// --- Configure Logger ---
	loggerArgs := configureLogger(logger)
	if loggerArgs.enabled {
		logger.Info("Request/Response logging enabled.")
		kfslogger.StartDispatcher(int(*workers), logger)
	} else {
		logger.Info("Request/Response logging disabled.")
	}
	// --- Configure Batcher ---
	batcherConfig := configureBatcher(logger) // Reads flags
	// --- Configure Cache ---
	cacheArgs := configureCache(logger)
	var inferenceCache *inferencecache.InferenceCache
	if cacheArgs.enabled {
		logger.Info("Inference Cache enabled.")
		inferenceCache = inferencecache.NewInferenceCache(cacheArgs.maxSizeBytes/1024, *cacheCleanupInterval)
		inferenceCache.SetDefaultTTL(time.Duration(cacheArgs.defaultTTLSeconds) * time.Second)
		defer inferenceCache.Close()
		logger.Infof("Cache Config: maxSizeMB=%d, defaultTTLSecs=%d", cacheArgs.maxSizeBytes/(1024*1024), cacheArgs.defaultTTLSeconds)
	} else {
		logger.Info("Inference Cache disabled.")
	}

	// --- Build Server and Handler Chain ---
	logger.Info("Building agent server...")
	serverComponents := &runtimeComponents{
		loggerArgs:  loggerArgs,
		batcherConf: batcherConfig,
		cache:       inferenceCache,
		probe:       probe,
	}
	mainServer, drain, metricsCollectors := buildServer(*port, *componentPort, serverComponents, logger)

	// --- Register Prometheus Metrics ---
	logger.Info("Registering Prometheus metrics...")
	prometheusRegistry := prom.DefaultRegisterer
	for _, collector := range metricsCollectors {
		if err := prometheusRegistry.Register(collector); err != nil {
			if are, ok := err.(prom.AlreadyRegisteredError); ok {
				logger.Warnf("Prom metric already registered: %v", are.ExistingCollector)
			} else {
				logger.Errorw("Failed to register Prom metric", zap.Error(err))
			}
		}
	}
	logger.Info("Prometheus metrics registered.")

	// --- Start HTTP Servers ---
	servers := map[string]*http.Server{"main": mainServer, "metrics": &http.Server{Addr: fmt.Sprintf(":%d", *metricsPort), Handler: promhttp.Handler()}}
	errCh := make(chan error, len(servers))
	listenCh := make(chan struct{})
	for name, server := range servers {
		go func(name string, s *http.Server) {
			logger.Infof("Starting %s server on %s", name, s.Addr)
			l, err := net.Listen("tcp", s.Addr)
			if err != nil {
				errCh <- fmt.Errorf("%s server failed to listen: %w", name, err)
				return
			}
			if s == mainServer {
				close(listenCh)
			}
			if err := s.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("%s server failed to serve: %w", name, err)
			}
			logger.Infof("%s server shut down.", name)
		}(name, server)
	}
	go func() {
		<-listenCh
		logger.Info("Starting unix socket listener on", unixSocketPath)
		if err := os.Remove(unixSocketPath); err != nil && !os.IsNotExist(err) {
			errCh <- fmt.Errorf("failed to remove existing unix socket %s: %w", unixSocketPath, err)
			return
		}
		l, err := net.Listen("unix", unixSocketPath)
		if err != nil {
			errCh <- fmt.Errorf("failed to listen to unix socket %s: %w", unixSocketPath, err)
			return
		}
		defer l.Close()
		unixServer := &http.Server{Handler: mainServer.Handler, ReadHeaderTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
		if err := unixServer.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serving failed on unix socket %s: %w", unixSocketPath, err)
		}
		logger.Info("Unix socket listener shut down.")
	}()

	// --- Wait for Signal or Server Error ---
	ctx := signals.NewContext()
	select {
	case err := <-errCh:
		logger.Errorw("Agent server failed unexpectedly, shutting down.", zap.Error(err))
		for _, srv := range servers {
			srv.Close()
		}
		os.Exit(1)
	case <-ctx.Done():
		logger.Info("Received TERM signal, attempting graceful shutdown...")
		if drain != nil {
			drain()
		} else {
			time.Sleep(drainSleepDuration)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for serverName, srv := range servers {
			logger.Info("Shutting down server: ", serverName)
			if err := srv.Shutdown(shutdownCtx); err != nil {
				logger.Errorw("Failed to gracefully shutdown server", zap.String("server", serverName), zap.Error(err))
			}
		}
		logger.Info("Shutdown complete, exiting...")
	}
}

// configureCache parses cache flags (reads flags directly)
func configureCache(logger *zap.SugaredLogger) cacheArgs {
	args := cacheArgs{enabled: *enableCache} // Use the flag value directly
	if !args.enabled {
		return args
	}

	args.maxSizeBytes = *cacheMaxSizeMb * 1024 * 1024 // Convert MiB from flag to Bytes
	if args.maxSizeBytes <= 0 {
		args.maxSizeBytes = 100 * 1024 * 1024
		logger.Warnf("Invalid cache-max-size-mb, using default 100MiB")
	}

	args.defaultTTLSeconds = *cacheDefaultTtlSeconds
	if args.defaultTTLSeconds <= 0 {
		args.defaultTTLSeconds = 300
		logger.Warnf("Invalid cache-default-ttl-seconds, using default 300s")
	}

	return args
}

// configureBatcher determines batcher type and config from flags
func configureBatcher(logger *zap.SugaredLogger) *batcherConfigHolder {
	holder := &batcherConfigHolder{enabled: *enableBatcher} // Use flag value

	if !holder.enabled {
		logger.Info("Batching disabled by flag.")
		return holder
	}

	useAdaptive := *enableAdaptiveBatcher

	// Validate required adaptive param if adaptive is chosen
	if useAdaptive && *targetLatency <= 0 {
		logger.Error("Adaptive batching enabled (--enable-adaptive-batcher) but required --target-latency flag is missing or invalid. Batching DISABLED.")
		holder.enabled = false
		return holder
	}

	if useAdaptive {
		logger.Info("Configuring Adaptive Batcher from flags...")
		cfg := batching.AdaptiveBatcherConfig{}

		// Populate from flags
		cfg.MaxBatchSize = *maxBatchSize
		cfg.InitialMaxBatchSize = *maxBatchSize
		cfg.MaxLatency = time.Duration(*maxLatency) * time.Millisecond
		cfg.InitialMaxLatency = cfg.MaxLatency
		cfg.MinBatchSize = *minBatchSize
		cfg.MinLatency = time.Duration(*minLatency) * time.Millisecond
		cfg.TargetLatency = time.Duration(*targetLatency) * time.Millisecond
		cfg.StateTransitionCoolDown = time.Duration(*stateTransitionCooldown) * time.Millisecond

		var err error
		cfg.TargetLatencyPercentile, err = strconv.ParseFloat(*targetLatencyPercentile, 64)
		if err != nil || cfg.TargetLatencyPercentile <= 0 || cfg.TargetLatencyPercentile >= 1 {
			logger.Warnw("Invalid --target-latency-percentile flag value, using default 0.95", "value", *targetLatencyPercentile, zap.Error(err))
			cfg.TargetLatencyPercentile = 0.95 // Use default
		}

		cfg.QueueLengthIncreaseBatchSizeThreshold, err = strconv.ParseFloat(*queueLengthThreshold, 64)
		if err != nil || cfg.QueueLengthIncreaseBatchSizeThreshold < 0 || cfg.QueueLengthIncreaseBatchSizeThreshold > 1 { // Allow 0 and 1? Or just between? Let's allow 0..1
			logger.Warnw("Invalid --queue-length-threshold flag value, using default 0.7", "value", *queueLengthThreshold, zap.Error(err))
			cfg.QueueLengthIncreaseBatchSizeThreshold = 0.7 // Use default
		}

		// Agent-wide adaptive defaults
		cfg.BatchSizeIncreaseStep = *adaptiveDefaultBatchSizeIncrStep
		cfg.LatencyIncreaseStep = time.Duration(*adaptiveDefaultLatencyIncrStepMs) * time.Millisecond
		cfg.WarningBatchSizeDecreaseFactor = *adaptiveDefaultWarnBSDecrFactor
		cfg.WarningLatencyDecreaseFactor = *adaptiveDefaultWarnLatDecrFactor
		cfg.DangerBatchSizeDecreaseFactor = *adaptiveDefaultDangerBSDecrFactor
		cfg.DangerLatencyDecreaseFactor = *adaptiveDefaultDangerLatDecrFactor

		// Hardcoded/Defaults for other fields
		cfg.LatencyWindowSize = batching.DefaultLatencyWindowSize         // Use internal default
		cfg.QueueLengthWindowSize = batching.DefaultQueueLengthWindowSize // Use internal default

		// Prometheus settings
		cfg.PrometheusNamespace = batching.DefaultPromNamespace
		cfg.PrometheusSubsystem = batching.DefaultPromSubsystem
		cfg.PrometheusLabels = map[string]string{"inferenceservice": *inferenceService, "namespace": *namespace, "endpoint": *endpoint, "component": *component}

		holder.config = cfg
		logger.Info("Adaptive batcher configured.")

	} else { // Configure Static Batcher
		logger.Info("Configuring Static Batcher from flags...")
		args := staticBatcherArgs{}
		args.maxBatchSize = *maxBatchSize
		args.maxLatency = *maxLatency

		// Ensure positive values
		if args.maxBatchSize <= 0 {
			args.maxBatchSize = batching.DefaultMaxBatchSize
			logger.Warnf("Invalid --max-batchsize, using default %d", args.maxBatchSize)
		}
		if args.maxLatency <= 0 {
			args.maxLatency = batching.DefaultMaxLatencyMs
			logger.Warnf("Invalid --max-latency, using default %dms", args.maxLatency)
		}

		holder.config = args
		logger.Infof("Static batcher configured: MaxBatchSize=%d, MaxLatency=%dms", args.maxBatchSize, args.maxLatency)
	}

	return holder
}

// configureLogger parses logger flags (reads flags directly)
func configureLogger(logger *zap.SugaredLogger) loggerArgs {
	args := loggerArgs{enabled: *enableLogger} // Use flag
	if !args.enabled {
		return args
	}

	// Validate Log Mode
	args.loggerType = v1beta1.LoggerType(*logMode)
	switch args.loggerType {
	case v1beta1.LogAll, v1beta1.LogRequest, v1beta1.LogResponse:
	default:
		logger.Warnf("Malformed log-mode '%s', defaulting to 'all'", *logMode)
		args.loggerType = v1beta1.LogAll
	}

	// Parse URLs
	var err error
	if *logUrl == "" {
		logger.Warn("Log URL is empty, logging disabled.")
		args.enabled = false
		return args
	}
	args.logUrl, err = url.Parse(*logUrl)
	if err != nil {
		logger.Errorw("Malformed log-url, logging disabled.", "url", *logUrl, zap.Error(err))
		args.enabled = false
		return args
	}

	sourceUriStr := *sourceUri
	if sourceUriStr == "" {
		sourceUriStr = fmt.Sprintf("http://localhost:%s/", *port)
	}
	args.sourceUrl, err = url.Parse(sourceUriStr)
	if err != nil {
		logger.Errorw("Malformed source-uri, logging disabled.", "uri", sourceUriStr, zap.Error(err))
		args.enabled = false
		return args
	}

	// Assign other args directly from flags
	args.inferenceService = *inferenceService
	args.endpoint = *endpoint
	args.namespace = *namespace
	args.component = *component
	args.metadataHeaders = *metadataHeaders
	args.certName = *CaCertFile
	args.tlsSkipVerify = *TlsSkipVerify

	return args
}

func startModelPuller(logger *zap.SugaredLogger) {
	downloader := agent.Downloader{
		ModelDir:  *modelDir,
		Providers: map[storage.Protocol]storage.Provider{},
		Logger:    logger,
	}
	watcher := agent.NewWatcher(*configDir, *modelDir, logger)
	logger.Info("Starting puller")
	agent.StartPullerAndProcessModels(&downloader, watcher.ModelEvents, logger)
	go watcher.Start()
}

func buildProbe(logger *zap.SugaredLogger, probeJSON string, autodetectHTTP2 bool, multiContainerProbes bool) *readiness.Probe {
	coreProbes, err := readiness.DecodeProbes(probeJSON, multiContainerProbes)
	if err != nil {
		logger.Fatalw("Agent failed to parse readiness probe", zap.Error(err))
		panic("Agent failed to parse readiness probe")
	}
	for _, probe := range coreProbes {
		if probe.InitialDelaySeconds == 0 {
			probe.InitialDelaySeconds = 10
		}
	}
	if autodetectHTTP2 {
		return readiness.NewProbeWithHTTP2AutoDetection(coreProbes)
	}
	newProbe := readiness.NewProbe(coreProbes)
	return newProbe
}

// runtimeComponents struct (remains the same)
type runtimeComponents struct {
	loggerArgs  loggerArgs
	batcherConf *batcherConfigHolder // Holds either static or adaptive config
	cache       *inferencecache.InferenceCache
	probe       func() bool
}

// buildServer (remains the same, uses logic in configure*)
func buildServer(port string, userPort int, components *runtimeComponents, logging *zap.SugaredLogger) (
	server *http.Server, drain func(), metrics []prom.Collector) {

	logging.Infof("Building server: agentPort=%s, userPort=%d", port, userPort)
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(userPort))}
	maxIdleConns := 1000
	httpProxy := httputil.NewSingleHostReverseProxy(target)
	httpProxy.Transport = pkgnet.NewAutoTransport(maxIdleConns, maxIdleConns)
	httpProxy.ErrorHandler = pkghandler.Error(logging)
	httpProxy.BufferPool = proxy.NewBufferPool()
	httpProxy.FlushInterval = proxy.FlushInterval

	var composedHandler http.Handler = httpProxy
	metrics = []prom.Collector{}

	// 3. Batcher (Decision Logic)
	if components.batcherConf != nil && components.batcherConf.enabled {
		switch cfg := components.batcherConf.config.(type) {
		case batching.AdaptiveBatcherConfig:
			logging.Info("Adding Adaptive Batcher middleware")
			adaptiveHandler := batching.New(composedHandler, logging, cfg, 100, batching.DefaultTickInterval)
			composedHandler = adaptiveHandler
			metrics = append(metrics, adaptiveHandler.GetMetrics()...)
		case staticBatcherArgs:
			logging.Info("Adding Static Batcher middleware")
			staticHandler := batcher.New(cfg.maxBatchSize, cfg.maxLatency, composedHandler, logging)
			composedHandler = staticHandler
		default:
			logging.Error("Unknown batcher configuration type, batching disabled.")
		}
	} else {
		logging.Info("Batcher middleware disabled.")
	}

	// 2. Cache (if enabled)
	if components.cache != nil {
		logging.Info("Adding Inference Cache middleware")
		cacheHandler := inferencecache.NewCacheHandler(components.cache, composedHandler)
		composedHandler = cacheHandler
		metrics = append(metrics, components.cache.GetMetrics()...)
	}

	// 1. Logger (if enabled)
	if components.loggerArgs.enabled {
		logging.Info("Adding Logger middleware")
		composedHandler = kfslogger.New(
			components.loggerArgs.logUrl, components.loggerArgs.sourceUrl, components.loggerArgs.loggerType,
			components.loggerArgs.inferenceService, components.loggerArgs.namespace, components.loggerArgs.endpoint,
			components.loggerArgs.component, composedHandler, components.loggerArgs.metadataHeaders,
			components.loggerArgs.certName, components.loggerArgs.tlsSkipVerify,
		)
	}

	composedHandler = queue.ForwardedShimHandler(composedHandler)
	drainer := &pkghandler.Drainer{QuietPeriod: drainSleepDuration, HealthCheckUAPrefixes: []string{header.ActivatorUserAgent}, Inner: composedHandler, HealthCheck: health.ProbeHandler(components.probe, false)}
	composedHandler = drainer

	server = pkgnet.NewServer(":"+port, composedHandler)
	drain = drainer.Drain
	return server, drain, metrics
}
