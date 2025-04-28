#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
# set -u # Be careful with this, might require explicit checks for optional vars
# Pipe failure should exit script
set -o pipefail

# --- Configuration ---
# Kubernetes Namespace where ISVCs are deployed
NAMESPACE="kserve-test"
# Ingress details (used to construct URL and Host header)
INGRESS_HOST="localhost"
INGRESS_PORT="8080"
# Base name for inference services (scenario suffix will be added)
ISVC_BASE_NAME="sklearn-iris"
# Directory to store benchmark results (CSV files)
RESULTS_DIR="../results_$(date +%Y%m%d_%H%M%S)" # Create results dir in parent folder
# Path to the compiled load tester executable
LOADTESTER_EXEC="./loadtester"
# Paths to ISVC YAML definitions relative to this script's location
BASELINE_YAML="../batcher/sklearn-iris-baseline.yaml"
STATIC_YAML="../batcher/sklearn-iris--static-batch.yaml"
ADAPTIVE_YAML="../batcher/sklearn-iris-adaptive-batch.yaml"
CACHE_ONLY_YAML="../batcher/isvc-cache-only.yaml"
CACHE_ADAPTIVE_YAML="../batcher/isvc-cache-adaptive.yaml"
# Paths to workload trace files relative to this script's location
BURSTY_TRACE="../workload/Bursty.txt"
PERIODIC_TRACE="../workload/Periodic.txt"

# Load Test Parameters (customize as needed)
DEFAULT_DURATION="1m"
DEFAULT_QPS="200" # Target INSTANCE QPS for constant load tests
DEFAULT_INSTANCES_PER_REQ="1"
DEFAULT_CONCURRENCY="50"
DEFAULT_PAYLOAD_POOL_SIZE="500"
DEFAULT_CACHE_HOTSET_RATIO="0.2"
DEFAULT_CACHE_HIT_RATIO="0.7" # Default hit ratio simulation for non-cache tests
CACHE_TEST_HIT_RATIO="0.95"    # Higher hit ratio simulation for cache tests
CACHE_TEST_HOTSET_RATIO="0.1"  # Smaller hot set for cache tests

# --- Helper Functions ---

log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $@"
}

log_error() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $@" >&2
}

# Deploys an ISVC and waits for it to be ready
# $1: ISVC name
# $2: Path to YAML file
deploy_and_wait() {
  local isvc_name="$1"
  local yaml_path="$2"
  log "Deploying ISVC '$isvc_name' from '$yaml_path'..."
  kubectl apply -f "$yaml_path" -n "$NAMESPACE"
  log "Waiting for ISVC '$isvc_name' to become ready..."
  if ! kubectl wait --for=condition=ready "inferenceservice/$isvc_name" -n "$NAMESPACE" --timeout=5m; then
    log_error "ISVC '$isvc_name' failed to become ready within timeout."
    # Try describing the ISVC for debugging before exiting
    kubectl describe "inferenceservice/$isvc_name" -n "$NAMESPACE" || true
    kubectl get pods -n "$NAMESPACE" -l "serving.kserve.io/inferenceservice=$isvc_name" || true
    exit 1
  fi
  log "ISVC '$isvc_name' is ready."
  # Give ingress/networking a few seconds to stabilize after readiness
  sleep 5
}

# Runs the load tester
# $1: Scenario name (for logging and output file)
# $2: Target ISVC name (used to construct URL/Host)
# $3: Load type ("qps" or "trace")
# $4: Load value (QPS number or path to trace file)
# $5: Duration (only for QPS, e.g., "1m") or Total Requests (only for trace, e.g., "0" for whole file)
# $6: Instances per request
# $7: Concurrency
# $8: Payload pool size
# $9: Payload hotset ratio
# $10: Payload cache hit ratio
run_load_test() {
  local scenario_name="$1"
  local isvc_name="$2"
  local load_type="$3"
  local load_value="$4"
  local duration_or_total="$5"
  local instances_per_req="$6"
  local concurrency="$7"
  local payload_pool_size="$8"
  local payload_hotset_ratio="$9"
  local payload_cache_hit_ratio="${10}"

  local target_url="http://${INGRESS_HOST}:${INGRESS_PORT}/v1/models/${isvc_name}:predict"
  local host_header="${isvc_name}.${NAMESPACE}.example.com" # Adjust domain if needed

  local output_csv="${RESULTS_DIR}/results_${scenario_name}.csv"
  local load_flags=""

  log "Starting load test for scenario: $scenario_name"
  log "  Target URL: $target_url"
  log "  Host Header: $host_header"

  if [[ "$load_type" == "qps" ]]; then
    local qps_val=$(echo "$load_value / $instances_per_req" | bc -l) # Calculate HTTP QPS
    qps_val=$(printf "%.0f" "$qps_val") # Round to integer QPS for flag
    if (( qps_val < 1 )); then qps_val=1; fi # Ensure at least 1 QPS for the flag
    load_flags="-qps $qps_val -duration $duration_or_total"
    log "  Load: Constant ${load_value} instance QPS (~${qps_val} HTTP req/s) for ${duration_or_total}"
  elif [[ "$load_type" == "trace" ]]; then
    load_flags="-trace-file $load_value"
    # For trace, duration/total-requests usually not needed unless limiting trace processing
    if [[ "$duration_or_total" != "0" ]]; then
        log_error "Duration/TotalRequests ($duration_or_total) typically not used with trace files, ignoring."
    fi
    log "  Load: Trace file '$load_value'"
  else
    log_error "Invalid load_type specified: $load_type"
    return 1
  fi

  # Construct the command
  local cmd=(
    "$LOADTESTER_EXEC"
    -url="$target_url"
    -host="$host_header"
    $load_flags
    -instances-per-req="$instances_per_req"
    -concurrency="$concurrency"
    -payload-pool-size="$payload_pool_size"
    -payload-hotset-ratio="$payload_hotset_ratio"
    -payload-cache-hit-ratio="$payload_cache_hit_ratio"
    -output-csv="$output_csv"
  )

  log "Executing: ${cmd[*]}"

  # Execute the load tester
  if ! "${cmd[@]}"; then
     log_error "Load test failed for scenario: $scenario_name"
     # Optionally keep the ISVC running for debugging? Or attempt cleanup anyway?
     # cleanup "$isvc_name" # Attempt cleanup even on failure
     # exit 1 # Stop the entire script on test failure
     return 1 # Allow script to continue with next scenario if desired
  fi

  log "Load test finished successfully for scenario: $scenario_name"
  log "Results saved to: $output_csv"
}

# Cleans up an ISVC
# $1: ISVC name
cleanup() {
  local isvc_name="$1"
  log "Cleaning up ISVC '$isvc_name'..."
  kubectl delete inferenceservice "$isvc_name" -n "$NAMESPACE" --ignore-not-found=true
  # Optional: Wait for deletion? Might take time.
  # kubectl wait --for=delete inferenceservice/$isvc_name -n $NAMESPACE --timeout=2m
  log "Cleanup for '$isvc_name' initiated."
  # Add a small pause between tests
  log "Pausing for 10 seconds before next test..."
  sleep 10
}

# --- Main Execution ---

log "Starting KServe Benchmark Script..."

# Check prerequisites
if ! command -v kubectl &> /dev/null; then
    log_error "kubectl command could not be found. Please install and configure kubectl."
    exit 1
fi
if [[ ! -x "$LOADTESTER_EXEC" ]]; then
    log_error "Load tester executable '$LOADTESTER_EXEC' not found or not executable. Please build it (go build loadtester.go)."
    exit 1
fi
if ! command -v bc &> /dev/null; then
    log_error "'bc' command not found. Please install it (needed for QPS calculation)."
    exit 1
fi


# Create results directory
mkdir -p "$RESULTS_DIR"
log "Results will be stored in: $RESULTS_DIR"

# --- Run Scenarios ---

# Scenario 1: Baseline (Constant QPS)
isvc_name_baseline="${ISVC_BASE_NAME}-baseline"
deploy_and_wait "$isvc_name_baseline" "$BASELINE_YAML"
run_load_test \
  "baseline_qps${DEFAULT_QPS}" \
  "$isvc_name_baseline" \
  "qps" \
  "$DEFAULT_QPS" \
  "$DEFAULT_DURATION" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$DEFAULT_CACHE_HOTSET_RATIO" \
  "$DEFAULT_CACHE_HIT_RATIO"
cleanup "$isvc_name_baseline"

# Scenario 2: Static Batching (Constant QPS)
isvc_name_static="${ISVC_BASE_NAME}-static-batch"
deploy_and_wait "$isvc_name_static" "$STATIC_YAML"
run_load_test \
  "static_qps${DEFAULT_QPS}" \
  "$isvc_name_static" \
  "qps" \
  "$DEFAULT_QPS" \
  "$DEFAULT_DURATION" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$DEFAULT_CACHE_HOTSET_RATIO" \
  "$DEFAULT_CACHE_HIT_RATIO"
cleanup "$isvc_name_static"

# Scenario 3: Adaptive Batching (Constant QPS)
isvc_name_adaptive="${ISVC_BASE_NAME}-adaptive-batch"
deploy_and_wait "$isvc_name_adaptive" "$ADAPTIVE_YAML"
run_load_test \
  "adaptive_qps${DEFAULT_QPS}" \
  "$isvc_name_adaptive" \
  "qps" \
  "$DEFAULT_QPS" \
  "$DEFAULT_DURATION" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$DEFAULT_CACHE_HOTSET_RATIO" \
  "$DEFAULT_CACHE_HIT_RATIO"
cleanup "$isvc_name_adaptive"

# Scenario 4: Cache Only (Constant QPS - High Hit Rate Simulation)
isvc_name_cache_only="${ISVC_BASE_NAME}-cache-only"
deploy_and_wait "$isvc_name_cache_only" "$CACHE_ONLY_YAML"
run_load_test \
  "cache_only_qps${DEFAULT_QPS}" \
  "$isvc_name_cache_only" \
  "qps" \
  "$DEFAULT_QPS" \
  "$DEFAULT_DURATION" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$CACHE_TEST_HOTSET_RATIO" \
  "$CACHE_TEST_HIT_RATIO"
cleanup "$isvc_name_cache_only"

# Scenario 5: Cache + Adaptive Batching (Constant QPS - High Hit Rate Simulation)
isvc_name_cache_adaptive="${ISVC_BASE_NAME}-cache-adaptive"
deploy_and_wait "$isvc_name_cache_adaptive" "$CACHE_ADAPTIVE_YAML"
run_load_test \
  "cache_adaptive_qps${DEFAULT_QPS}" \
  "$isvc_name_cache_adaptive" \
  "qps" \
  "$DEFAULT_QPS" \
  "$DEFAULT_DURATION" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$CACHE_TEST_HOTSET_RATIO" \
  "$CACHE_TEST_HIT_RATIO"
cleanup "$isvc_name_cache_adaptive"


# --- Add Trace File Scenarios ---

log "Running Trace File Scenarios..."

# Scenario 6: Adaptive Batching (Bursty Trace)
deploy_and_wait "$isvc_name_adaptive" "$ADAPTIVE_YAML" # Redeploy adaptive
run_load_test \
  "adaptive_trace_bursty" \
  "$isvc_name_adaptive" \
  "trace" \
  "$BURSTY_TRACE" \
  "0" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$DEFAULT_CACHE_HOTSET_RATIO" \
  "$DEFAULT_CACHE_HIT_RATIO"
cleanup "$isvc_name_adaptive"

# Scenario 7: Adaptive Batching (Periodic Trace)
deploy_and_wait "$isvc_name_adaptive" "$ADAPTIVE_YAML" # Redeploy adaptive
run_load_test \
  "adaptive_trace_periodic" \
  "$isvc_name_adaptive" \
  "trace" \
  "$PERIODIC_TRACE" \
  "0" \
  "$DEFAULT_INSTANCES_PER_REQ" \
  "$DEFAULT_CONCURRENCY" \
  "$DEFAULT_PAYLOAD_POOL_SIZE" \
  "$DEFAULT_CACHE_HOTSET_RATIO" \
  "$DEFAULT_CACHE_HIT_RATIO"
cleanup "$isvc_name_adaptive"

# Add more scenarios for static/baseline with traces if needed...


log "All benchmark scenarios completed."
log "Results are stored in: $RESULTS_DIR"

exit 0