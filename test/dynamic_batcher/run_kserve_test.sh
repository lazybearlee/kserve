#!/bin/bash

# File: run_kserve_test.sh
# Usage: ./run_kserve_test.sh [OPTIONS]

DEFAULT_TRACE_FILE="./workload_trace.txt"
DEFAULT_INPUT_FILE="./iris-input.json"
DEFAULT_NAMESPACE="kserve-test"
DEFAULT_SERVICE_NAME="sklearn-iris"
DEFAULT_MODEL_NAME="sklearn-iris"
DEFAULT_WORKERS=20
DEFAULT_PORT=8080

# 自动获取 Istio ingress 服务名称
INGRESS_SERVICE=$(kubectl get svc -n istio-system --selector=app=istio-ingressgateway -o jsonpath='{.items[0].metadata.name}')

# 参数解析
while [[ $# -gt 0 ]]; do
  case "$1" in
    --trace-file)
      TRACE_FILE="$2"
      shift 2
      ;;
    --input-file)
      INPUT_FILE="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --service-name)
      SERVICE_NAME="$2"
      shift 2
      ;;
    --model-name)
      MODEL_NAME="$2"
      shift 2
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# 设置默认值
TRACE_FILE=${TRACE_FILE:-$DEFAULT_TRACE_FILE}
INPUT_FILE=${INPUT_FILE:-$DEFAULT_INPUT_FILE}
NAMESPACE=${NAMESPACE:-$DEFAULT_NAMESPACE}
SERVICE_NAME=${SERVICE_NAME:-$DEFAULT_SERVICE_NAME}
MODEL_NAME=${MODEL_NAME:-$DEFAULT_MODEL_NAME}
WORKERS=${WORKERS:-$DEFAULT_WORKERS}
PORT=${PORT:-$DEFAULT_PORT}

# 验证必要文件存在
check_file() {
  if [ ! -f "$1" ]; then
    echo "ERROR: File $1 not found!"
    exit 1
  fi
}

check_file "$TRACE_FILE"
check_file "$INPUT_FILE"

# 自动设置端口转发
setup_port_forward() {
  echo "Starting port-forward on port $PORT..."
  kubectl port-forward -n istio-system svc/$INGRESS_SERVICE $PORT:80 > /dev/null &
  PF_PID=$!
  sleep 2  # 等待端口转发就绪
  echo "Port-forward running with PID $PF_PID"
}

# 清理函数
cleanup() {
  echo "Stopping port-forward..."
  kill $PF_PID 2>/dev/null
  exit 0
}

trap cleanup SIGINT SIGTERM

# 主执行逻辑
setup_port_forward

echo "Starting KServe workload test with configuration:"
echo "-----------------------------------------------"
echo "Trace file:      $TRACE_FILE"
echo "Input file:      $INPUT_FILE"
echo "Namespace:       $NAMESPACE"
echo "Service name:    $SERVICE_NAME"
echo "Model name:      $MODEL_NAME"
echo "Workers:         $WORKERS"
echo "Port:            $PORT"
echo "-----------------------------------------------"

python3 kserve_trace_simulator.py \
  --trace-file "$TRACE_FILE" \
  --input-file "$INPUT_FILE" \
  --host localhost \
  --port "$PORT" \
  --model-name "$MODEL_NAME" \
  --namespace "$NAMESPACE" \
  --service-name "$SERVICE_NAME" \
  --workers "$WORKERS"

cleanup