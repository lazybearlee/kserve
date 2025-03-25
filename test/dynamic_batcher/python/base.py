import argparse
import time
import requests
import json


def main():
    parser = argparse.ArgumentParser(description="KServe Workload Simulator")
    parser.add_argument(
        "--input-file", type=str, required=True, help="Path to workload timing file"
    )
    parser.add_argument(
        "--data-file", type=str, required=True, help="Path to input JSON data file"
    )
    parser.add_argument(
        "--ingress-host", type=str, default="localhost", help="Ingress host address"
    )
    parser.add_argument(
        "--ingress-port", type=int, default=8080, help="Ingress port number"
    )
    parser.add_argument(
        "--service-hostname", type=str, required=True, help="Inference service hostname"
    )
    parser.add_argument(
        "--model-name", type=str, required=True, help="Name of the model to invoke"
    )
    parser.add_argument(
        "--namespace", type=str, help="Namespace (for logging purposes)"
    )

    args = parser.parse_args()

    # 读取工作负载间隔时间
    with open(args.input_file, "r") as f:
        intervals = [float(line.strip()) for line in f if line.strip()]

    # 读取输入数据
    with open(args.data_file, "r") as f:
        payload = json.load(f)

    # 构建请求URL
    url = f"http://{args.ingress_host}:{args.ingress_port}/v1/models/{args.model_name}:predict"

    # 设置请求头
    headers = {"Host": args.service_hostname, "Content-Type": "application/json"}

    # 发送请求
    for idx, interval in enumerate(intervals):
        try:
            start_time = time.time()
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            elapsed = time.time() - start_time

            print(
                f"[{idx+1}/{len(intervals)}] "
                f"Status: {response.status_code} "
                f"Time: {elapsed:.2f}s "
                f"Next delay: {interval:.2f}s"
            )

        except Exception as e:
            print(f"[{idx+1}/{len(intervals)}] Error: {str(e)}")

        # 按照工作负载间隔等待
        time.sleep(interval)


if __name__ == "__main__":
    main()
