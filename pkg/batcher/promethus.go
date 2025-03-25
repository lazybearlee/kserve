package batcher

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// promQueryResponse 用于解析 Prometheus HTTP API 返回的 JSON 结构
type promQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			// Value 格式为 [ <timestamp>, "<value>" ]
			Value [2]interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// getBaseLatency 使用 PromQL 查询计算最近1分钟内推理的平均延迟（单位：秒）
// 此处查询使用的是 QueueProxy 指标 request_predict_seconds_sum 与 request_predict_seconds_count
func getBaseLatency() (float64, error) {
	queryStr := `avg(rate(request_predict_seconds_sum[1m])) / avg(rate(request_predict_seconds_count[1m]))`
	u := "http://localhost:8888/api/v1/query?query=" + url.QueryEscape(queryStr)
	resp, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var result promQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, fmt.Errorf("no data returned")
	}
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("cannot parse latency value")
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// getAverageLatency 查询 PromQL 获取当前平均延迟（单位：秒），逻辑与 updateBaseLatency 类似
func getAverageLatency() (float64, error) {
	queryStr := `avg(rate(revision_request_latencies_sum[1m])) / avg(rate(revision_request_latencies_count[1m]))`
	u := "http://localhost:8888/api/v1/query?query=" + url.QueryEscape(queryStr)
	resp, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var result promQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, fmt.Errorf("no data returned")
	}
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("cannot parse latency value")
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// getArrivalRate 使用 PromQL 查询队列代理请求到达率（单位：请求/秒）
func getArrivalRate() (float64, error) {
	queryStr := `avg(rate(revision_request_count[1m]))`
	u := "http://localhost:8888/api/v1/query?query=" + url.QueryEscape(queryStr)
	resp, err := http.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	var result promQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, fmt.Errorf("no data returned")
	}
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("cannot parse arrival rate value")
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}
