# KServe Performance Benchmark Suite

This suite automates performance testing for KServe InferenceServices under various configurations, including different batching strategies and caching, using a custom Go load generator.

## Prerequisites

1.  **Kubernetes Cluster:** Access to a Kubernetes cluster where KServe is installed and running.
2.  **`kubectl`:** Configured to interact with your cluster.
3.  **Go:** Go programming language (version 1.18 or later recommended) installed locally to build the load tester.
4.  **KServe Installation:** KServe controllers and required dependencies must be installed in the cluster.
5.  **Ingress:** A working Ingress controller (like Istio, Kourier, etc.) configured for KServe, and knowledge of how to access services through it (e.g., via `localhost:8080` port-forwarding or an external IP/domain).
6.  **Prometheus/Grafana (Recommended):** For monitoring agent-specific metrics (adaptive isvc parameters, cache statistics) and system metrics during the tests.

## Directory Structure

```
test/exp_benchmark/
├── README.md
├── isvc/                  # ISVC YAML definitions for different scenarios
│   ├── isvc-baseline.yaml
│   ├── isvc-static-batching.yaml
│   └── isvc-adaptive-batching.yaml
│   └── isvc-cache-only.yaml
│   └── isvc-cache-adaptive.yaml
├── script/                   # Scripts for running benchmarks
│   ├── loadtester.go         # Go load testing script source
│   ├── loadtester            # Compiled executable (after building)
│   └── run_benchmarks.sh     # Main benchmark execution script
└── workload/                 # Workload definitions (trace files)
    ├── Bursty.txt
    └── Periodic.txt
└── results_YYYYMMDD_HHMMSS/  # Output directory (created by script)
    ├── results_baseline_qpsXXX.csv
    ├── results_static_qpsXXX.csv
    └── ...                   # CSV result files for each scenario run
```

## Configuration

Before running, configure the `run_benchmarks.sh` script:

1.  **Edit Variables:** Open `script/run_benchmarks.sh` and modify the variables in the `--- Configuration ---` section:
    *   `NAMESPACE`: The Kubernetes namespace where your InferenceServices will be deployed and tested.
    *   `INGRESS_HOST`, `INGRESS_PORT`: How you access your cluster's ingress gateway from where you run the script (e.g., `localhost`, `8080` if using port-forward).
    *   `ISVC_BASE_NAME`: The base name for the test InferenceServices (e.g., `sklearn-iris`).
    *   `RESULTS_DIR`: Where the output CSV files will be saved (relative to the script location).
    *   `LOADTESTER_EXEC`: Path to the compiled `loadtester` executable.
    *   `*_YAML`, `*_TRACE`: Paths to your scenario definition files and trace files.
    *   `DEFAULT_*` Parameters: Adjust the default QPS, duration, concurrency, payload settings, etc., for the tests.

2.  **YAML Definitions:** Ensure the `.yaml` files in the `isvc/` directory define the correct InferenceService configurations for each scenario (baseline, static, adaptive, cache). Pay attention to the `metadata.name` (it should match `<ISVC_BASE_NAME>-<scenario>`), `isvc`, and `cache` sections.

3.  **Trace Files:** If using trace files, ensure they are correctly formatted with inter-arrival times (in seconds) per line in the `workload/` directory.

## Building the Load Tester

Navigate to the `script/` directory and build the Go program:

```bash
cd test/exp_benchmark/script
go build loadtester.go
# Ensure the script is executable
chmod +x run_benchmarks.sh
```

## Running the Benchmark

1.  **Navigate:** Go to the `script/` directory.
2.  **Execute:** Run the main script:
    ```bash
    ./run_benchmarks.sh
    ```
3.  **Monitor:** The script will print progress logs to the console. You can monitor:
    *   `kubectl get inferenceservice -n <NAMESPACE>` to see deployment status.
    *   `kubectl get pods -n <NAMESPACE>` to see pod status.
    *   Prometheus/Grafana dashboards for detailed metrics (if configured).

## Output

*   **Console:** The script logs its progress, including deployment, test execution, and cleanup steps. The `loadtester` output summary (throughput, latency, etc.) for each scenario is printed to the console upon completion.
*   **CSV Files:** Raw per-request results (timestamp, latency, status code, instances, error) for each scenario run are saved as `.csv` files inside the `results_YYYYMMDD_HHMMSS/` directory (created automatically).

## Scenarios Tested (Default)

The default `run_benchmarks.sh` script executes the following scenarios:

1.  **Baseline (Constant QPS):** No batching, no cache.
2.  **Static Batching (Constant QPS):** Uses settings from `isvc-static-batching.yaml`.
3.  **Adaptive Batching (Constant QPS):** Uses settings from `isvc-adaptive-batching.yaml`.
4.  **Cache Only (Constant QPS):** Uses settings from `isvc-cache-only.yaml`, simulates high cache hit rate.
5.  **Cache + Adaptive Batching (Constant QPS):** Uses settings from `isvc-cache-adaptive.yaml`, simulates high cache hit rate.
6.  **Adaptive Batching (Bursty Trace):** Uses trace file `Bursty.txt`.
7.  **Adaptive Batching (Periodic Trace):** Uses trace file `Periodic.txt`.

## Customization

*   **Modify Scenarios:** Edit the `main` execution block in `run_benchmarks.sh` to add, remove, or modify scenarios and their parameters (QPS, duration, trace file, cache simulation ratios, etc.).
*   **Change Configs:** Update the `.yaml` files in `isvc/` to test different batching/caching parameters.
*   **Load Tester:** Modify `loadtester.go` to change payload generation, metrics collection, or reporting logic. Recompile after changes.

## Notes and Considerations

*   **Resource Isolation:** Ensure only **one** test InferenceService is running at a time to avoid resource contention influencing results. The script handles cleanup automatically.
*   **Cluster State:** Run tests on a stable cluster with sufficient resources. Monitor node CPU/memory during tests.
*   **Warm-up:** Consider adding a short warm-up phase before each main load test run for more stable results.
*   **Prometheus:** Monitoring agent metrics via Prometheus is essential for understanding *why* adaptive batching or caching performs the way it does (e.g., actual batch sizes formed, cache hit/miss rates).
*   **Result Interpretation:** Pay close attention to **Instance Throughput** in addition to HTTP Request Throughput when evaluating batching effectiveness. Compare latency percentiles (p50, p95, p99) across scenarios.