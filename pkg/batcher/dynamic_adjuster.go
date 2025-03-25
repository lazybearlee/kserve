package batcher

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// 参数定义
var (
	alpha         = 1.0  // 延迟惩罚权重
	beta          = 1.0  // 吞吐奖励权重
	lambdaPenalty = 10.0 // 延迟超标惩罚系数

	// 系统初始估计参数，L0 由指标更新
	L0     = 50.0 // 基本延迟(ms)
	gamma1 = 0.5  // 每增加1个B增加的延迟(ms)
	gamma2 = 1.0  // 每个请求队列增加的延迟(ms)
	delta  = 0.1  // 吞吐增长参数

	L_target = 150.0 // SLA规定的最大延迟 (ms)

	adjustment_interval = 5 * time.Second // 调节周期

	Bmax = 32 // 系统允许的最大批处理大小
)

// F 目标函数计算：F(B, Q)= α*(L0+γ1*B+γ2*Q) + λ*max(0, L - L_target) - beta*(r*(1-exp(-δ*B)))
func F(B int, Q float64, r float64) float64 {
	L := L0 + gamma1*float64(B) + gamma2*Q
	penalty := lambdaPenalty * math.Max(0, L-L_target)
	throughput := r * (1 - math.Exp(-delta*float64(B)))
	return alpha*L + penalty - beta*throughput
}

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

// updateBaseLatency 使用 PromQL 查询计算最近1分钟内推理的平均延迟（单位：毫秒）
// 此处查询使用的是 QueueProxy 指标 request_predict_seconds_sum 与 request_predict_seconds_count
func updateBaseLatency() float64 {
	queryStr := `avg(rate(request_predict_seconds_sum[1m])) / avg(rate(request_predict_seconds_count[1m]))`
	u := "http://localhost:8888/api/v1/query?query=" + url.QueryEscape(queryStr)
	resp, err := http.Get(u)
	if err != nil {
		return L0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return L0
	}
	var result promQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return L0
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return L0
	}
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return L0
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return L0
	}
	return val * 1000 // 单位转换为毫秒
}

// getAverageLatency 查询 PromQL 获取当前平均延迟（单位：毫秒），逻辑与 updateBaseLatency 类似
func getAverageLatency(logger *zap.SugaredLogger) (float64, error) {
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
	// 单位为毫秒
	return val, nil
}

// updateArrivalRate 使用 PromQL 查询队列代理请求到达率（单位：请求/秒）
func updateArrivalRate() float64 {
	queryStr := `avg(rate(revision_request_count[1m]))`
	u := "http://localhost:8888/api/v1/query?query=" + url.QueryEscape(queryStr)
	resp, err := http.Get(u)
	if err != nil {
		return 100.0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 100.0
	}
	var result promQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return 100.0
	}
	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 100.0
	}
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 100.0
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 100.0
	}
	return val
}

// 自动调整参数权重，根据当前监控的平均延迟及到达率进行自适应调整
func autoAdjustParamWeights(logger *zap.SugaredLogger) {
	avgLatency, err := getAverageLatency(logger)
	if err != nil {
		logger.Warnf("AutoAdjust: failed to get average latency: %v", err)
		return
	}

	// 根据平均延迟自动调整 lambdaPenalty
	if avgLatency > L_target*1.1 {
		oldLambda := lambdaPenalty
		lambdaPenalty *= 1.1 // 延迟高则提高延迟惩罚权重
		logger.Infof("AutoAdjust: increasing lambdaPenalty from %.2f to %.2f due to high avg latency %.2f ms", oldLambda, lambdaPenalty, avgLatency)
	} else if avgLatency < L_target*0.9 {
		oldLambda := lambdaPenalty
		lambdaPenalty *= 0.9 // 延迟低则降低延迟惩罚权重
		logger.Infof("AutoAdjust: decreasing lambdaPenalty from %.2f to %.2f due to low avg latency %.2f ms", oldLambda, lambdaPenalty, avgLatency)
	}

	// 调整 alpha：如果延迟明显超过目标值，提高 alpha，增加延迟惩罚的比重
	if avgLatency > L_target*1.05 {
		oldAlpha := alpha
		alpha *= 1.05
		logger.Infof("AutoAdjust: increasing alpha from %.2f to %.2f", oldAlpha, alpha)
	} else if avgLatency < L_target*0.95 {
		oldAlpha := alpha
		alpha *= 0.95
		logger.Infof("AutoAdjust: decreasing alpha from %.2f to %.2f", oldAlpha, alpha)
	}

	// 调整 beta：根据到达率调整吞吐奖励权重
	arrivalRate := updateArrivalRate()
	if arrivalRate > 120 {
		oldBeta := beta
		beta *= 1.05
		logger.Infof("AutoAdjust: increasing beta from %.2f to %.2f due to high arrival rate %.2f req/s", oldBeta, beta, arrivalRate)
	} else if arrivalRate < 80 {
		oldBeta := beta
		beta *= 0.95
		logger.Infof("AutoAdjust: decreasing beta from %.2f to %.2f due to low arrival rate %.2f req/s", oldBeta, beta, arrivalRate)
	}
}

func (handler *BatchHandler) getCurrentQueueLength() float64 {
	// 直接从当前BatchHandler实例中获取队列长度
	handler.InfoRwMutex.RLock()
	defer handler.InfoRwMutex.RUnlock()
	return float64(handler.batcherInfo.CurrentInputLen)
}

// setDynamicBatchSize 更新 BatchHandler 内使用的动态批处理大小
func (handler *BatchHandler) setDynamicBatchSize(newSize int) {
	if newSize < 1 {
		newSize = 1
	}
	if newSize > handler.MaxBatchSize {
		newSize = handler.MaxBatchSize
	}
	handler.log.Infof("Dynamic adjust: setting max batch size to %d", newSize)
	handler.BatchSize = newSize
}

// AdjustDynamicBatchSizeLoop 主动态调节循环
func (handler *BatchHandler) AdjustDynamicBatchSizeLoop() {
	ticker := time.NewTicker(adjustment_interval)
	defer ticker.Stop()

	for {
		<-ticker.C

		// 先自动调整参数权重
		autoAdjustParamWeights(handler.log)

		// 获取当前队列长度、更新基础延迟及到达率
		Q := handler.getCurrentQueueLength()
		L0 = updateBaseLatency()
		r := updateArrivalRate()

		bestB := 1
		bestF := F(1, Q, r)
		for B := 2; B <= handler.MaxBatchSize; B++ {
			currentF := F(B, Q, r)
			if currentF < bestF {
				bestF = currentF
				bestB = B
			}
		}
		handler.log.Infof("Dynamic adjust: Q=%.2f, L0=%.2f, r=%.2f, bestB=%d, bestF=%.2f", Q, L0, r, bestB, bestF)
		handler.setDynamicBatchSize(bestB)
	}
}
