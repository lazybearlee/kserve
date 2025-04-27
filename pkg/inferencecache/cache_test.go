package inferencecache

import (
	"bytes"
	rand2 "crypto/rand"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 辅助函数：创建随机数据
func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand2.Read(b)
	return b
}

// 辅助函数：创建测试请求
func createTestRequest(method, path string, body []byte) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

// 测试缓存基本功能
func TestCacheBasics(t *testing.T) {
	cache := NewInferenceCache(10, time.Minute)
	defer cache.Close()

	// 测试Set和Get
	key := "test-key"
	value := []byte("test-value")
	cache.Set(key, value, 200, http.Header{"Content-Type": []string{"text/plain"}}, time.Minute)

	entry, found := cache.Get(key)
	if !found {
		t.Fatalf("Expected to find key in cache, but didn't")
	}
	if string(entry.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got %s", string(entry.Value))
	}
	if entry.StatusCode != 200 {
		t.Errorf("Expected status code 200, got %d", entry.StatusCode)
	}

	// 测试过期
	key2 := "expire-key"
	cache.Set(key2, []byte("expire-value"), 200, http.Header{}, 50*time.Millisecond)

	// 等待过期
	time.Sleep(100 * time.Millisecond)

	_, found = cache.Get(key2)
	if found {
		t.Errorf("Expected key to be expired, but it was found")
	}
}

// 测试LRU淘汰
func TestCacheLRU(t *testing.T) {
	// 创建一个小的缓存（1KB）
	cache := NewInferenceCache(1, time.Minute)
	defer cache.Close()

	// 填充缓存，应该会触发淘汰
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := randomBytes(200) // 每个条目约200字节，200*10=2KB
		cache.Set(key, value, 200, http.Header{}, time.Minute)
	}

	// 验证缓存大小没有超过限制
	stats := cache.Stats()
	if stats["currentSize"].(int) > stats["maxSizeBytes"].(int) {
		t.Errorf("Cache exceeded max size: %d > %d",
			stats["currentSize"].(int), stats["maxSizeBytes"].(int))
	}

	// 验证早期的键已被淘汰
	_, found := cache.Get("key-0")
	if found {
		t.Errorf("Expected key-0 to be evicted, but it was found")
	}
}

// 测试并发安全性
func TestCacheConcurrency(t *testing.T) {
	cache := NewInferenceCache(10, time.Minute)
	defer cache.Close()

	wg := sync.WaitGroup{}
	concurrency := 100
	operations := 1000

	// 并发读写测试
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operations; j++ {
				// 随机读或写操作
				if rand.Intn(2) == 0 {
					key := fmt.Sprintf("key-%d-%d", id, j)
					value := []byte(fmt.Sprintf("value-%d-%d", id, j))
					cache.Set(key, value, 200, http.Header{}, time.Minute)
				} else {
					key := fmt.Sprintf("key-%d-%d", rand.Intn(id+1), rand.Intn(j+1))
					cache.Get(key)
				}
			}
		}(i)
	}

	wg.Wait()
	// 如果没有panic，测试通过
}

// 测试HTTP处理器
func TestCacheHandler(t *testing.T) {
	cache := NewInferenceCache(10, time.Minute)
	defer cache.Close()

	// 创建测试处理器
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 模拟推理处理
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"prediction": "test-result for input: %d"}`, time.Now().UnixNano())))
	})

	cacheHandler := NewCacheHandler(cache, handler)

	// 创建测试请求
	req := createTestRequest("POST", "/inference", []byte(`{"input": "test"}`))

	// 第一次请求 - 缓存未命中
	rr := httptest.NewRecorder()
	cacheHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}

	firstResponse := rr.Body.String()

	// 第二次请求 - 应该命中缓存
	req = createTestRequest("POST", "/inference", []byte(`{"input": "test"}`))
	rr = httptest.NewRecorder()
	cacheHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}

	secondResponse := rr.Body.String()

	// 验证响应相同
	if firstResponse != secondResponse {
		t.Errorf("Expected identical responses, got:\nFirst: %s\nSecond: %s",
			firstResponse, secondResponse)
	}
}

// 性能测试 - 写入缓存
func BenchmarkCacheSet(b *testing.B) {
	cache := NewInferenceCache(100, time.Hour)
	defer cache.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-key-" + strconv.Itoa(i)
		value := []byte("bench-value-" + strconv.Itoa(i))
		cache.Set(key, value, 200, http.Header{}, time.Minute)
	}
}

// 性能测试 - 读取缓存
func BenchmarkCacheGet(b *testing.B) {
	cache := NewInferenceCache(100, time.Hour)
	defer cache.Close()

	// 预填充缓存
	for i := 0; i < 1000; i++ {
		key := "bench-key-" + strconv.Itoa(i)
		value := []byte("bench-value-" + strconv.Itoa(i))
		cache.Set(key, value, 200, http.Header{}, time.Hour)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "bench-key-" + strconv.Itoa(i%1000)
		cache.Get(key)
	}
}

// 性能测试 - HTTP处理器（模拟真实推理场景）
func BenchmarkCacheHandler(b *testing.B) {
	cache := NewInferenceCache(100, time.Hour)
	defer cache.Close()

	// 创建模拟的推理处理器（速度快）
	fastHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"prediction": "fast-result"}`))
	})

	// 创建模拟的推理处理器（速度慢）
	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond) // 模拟耗时计算
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"prediction": "slow-result"}`))
	})

	// 测试不同场景下的性能
	benchCases := []struct {
		name    string
		handler http.Handler
	}{
		{"FastNoCache", fastHandler},
		{"FastWithCache", NewCacheHandler(cache, fastHandler)},
		{"SlowNoCache", slowHandler},
		{"SlowWithCache", NewCacheHandler(cache, slowHandler)},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			cache.Close() // 重置缓存
			cache = NewInferenceCache(100, time.Hour)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				req := createTestRequest("POST", "/inference", []byte(`{"input": "test"}`))
				rr := httptest.NewRecorder()
				bc.handler.ServeHTTP(rr, req)
				if rr.Code != http.StatusOK {
					b.Fatalf("Expected status 200, got %d", rr.Code)
				}
				// 确保完全读取响应
				_, _ = ioutil.ReadAll(rr.Body)
			}
		})
	}
}

// 性能测试 - 高并发场景
func BenchmarkCacheConcurrency(b *testing.B) {
	cache := NewInferenceCache(100, time.Hour)
	defer cache.Close()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond) // 模拟轻度计算
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"prediction": "concurrent-result"}`))
	})

	cacheHandler := NewCacheHandler(cache, handler)

	// 创建固定的请求集
	requests := make([]*http.Request, 100)
	for i := 0; i < 100; i++ {
		requests[i] = createTestRequest("POST", "/inference",
			[]byte(fmt.Sprintf(`{"input": "test-%d"}`, i%10))) // 10种不同输入
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			req := requests[i%len(requests)]
			i++
			rr := httptest.NewRecorder()
			cacheHandler.ServeHTTP(rr, req)
		}
	})
}
