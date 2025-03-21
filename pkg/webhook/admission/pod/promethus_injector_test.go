package pod

import (
	"errors"
	"testing"

	"github.com/onsi/gomega"
	"knative.dev/pkg/kmp"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kserve/kserve/pkg/constants"
)

const (
	testPromImage     = "prometheus:latest"
	testCpuRequest    = "100m"
	testCpuLimit      = "200m"
	testMemoryRequest = "128Mi"
	testMemoryLimit   = "256Mi"
	testConfigContent = "global:\n  scrape_interval: 15s\n"
)

func TestPrometheusInjector(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scenarios := map[string]struct {
		original *corev1.Pod
		// 对比注重检查: 新增卷、init container、prometheus sidecar、以及对 agent 容器的参数/env修改
		expected func(pod *corev1.Pod) error
	}{
		"InjectPrometheus": {
			original: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "user-namespace", // 用户自定义命名空间
					Annotations: map[string]string{
						constants.BatcherInternalAnnotationKey:    "true",
						constants.PrometheusInternalAnnotationKey: "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "predictor",
						},
						{
							Name: constants.AgentContainerName,
						},
					},
				},
			},
			expected: func(pod *corev1.Pod) error {
				// 验证 volumes 中加入了 prometheus-config 卷
				var volFound bool
				for _, vol := range pod.Spec.Volumes {
					if vol.Name == PrometheusConfigVolumeName {
						volFound = true
						break
					}
				}
				if !volFound {
					if gomega.Expect(volFound).To(gomega.BeTrue(), "expected volume %q not found", PrometheusConfigVolumeName) {
						return nil
					}
					return errors.New("expected volume not found")
				}
				// 验证 init container 存在并且写入了正确 env变量
				var initFound bool
				for _, ic := range pod.Spec.InitContainers {
					if ic.Name == "prometheus-config-init" {
						initFound = true
						// 检查 env PROMETHEUS_CONFIG_FILE_PATH 是否正确
						var envFound bool
						for _, env := range ic.Env {
							if env.Name == "PROMETHEUS_CONFIG_FILE_PATH" && env.Value == PrometheusConfigMountPath+"/prometheus.yml" {
								envFound = true
								break
							}
						}
						g.Expect(envFound).To(gomega.BeTrue(), "prometheus-config-init env PROMETHEUS_CONFIG_FILE_PATH not set correctly")
						break
					}
				}
				g.Expect(initFound).To(gomega.BeTrue(), "expected init container prometheus-config-init not found")

				// 验证 Prometheus sidecar容器
				var promFound bool
				for _, c := range pod.Spec.Containers {
					if c.Name == PrometheusContainerName {
						promFound = true
						// 检查 Args 是否包含指定的配置文件路径
						g.Expect(c.Args).To(gomega.ContainElement("--config.file=/etc/prometheus/prometheus.yml"))
						// 检查 VolumeMount 是正确的（挂载到 /etc/prometheus 且只读）
						var vmFound bool
						for _, vm := range c.VolumeMounts {
							if vm.Name == PrometheusConfigVolumeName && vm.MountPath == PrometheusConfigMountPath && vm.ReadOnly {
								vmFound = true
								break
							}
						}
						g.Expect(vmFound).To(gomega.BeTrue(), "expected VolumeMount in Prometheus container not found")
						break
					}
				}
				g.Expect(promFound).To(gomega.BeTrue(), "expected Prometheus container not injected")

				// 验证对 agent 容器的修改
				var agentModified bool
				for i, c := range pod.Spec.Containers {
					if c.Name == constants.AgentContainerName {
						// 检查启动参数
						g.Expect(c.Args).To(gomega.ContainElement("--prometheus-scrape-url=http://localhost:9090/metrics"))
						// 检查环境变量
						var envFound bool
						for _, env := range c.Env {
							if env.Name == "PROMETHEUS_SCRAPE_URL" && env.Value == "http://localhost:9090/metrics" {
								envFound = true
								break
							}
						}
						g.Expect(envFound).To(gomega.BeTrue(), "expected environment variable PROMETHEUS_SCRAPE_URL not set in agent container")
						// 将修改后的 agent 赋回（方便 kmp 比较时忽略其他字段）
						pod.Spec.Containers[i] = c
						agentModified = true
						break
					}
				}
				g.Expect(agentModified).To(gomega.BeTrue(), "agent container not found for modification")
				return nil
			},
		},
		"DoNotInjectPrometheusDueToMissingAnnotation": {
			original: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "user-namespace",
					Annotations: map[string]string{
						// 仅设置 batcher 注解，不设置 Prometheus 注解
						constants.BatcherInternalAnnotationKey: "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "predictor",
						},
						{
							Name: constants.AgentContainerName,
						},
					},
				},
			},
			expected: func(pod *corev1.Pod) error {
				// Pod 没有注入 Prometheus 容器及 init container
				for _, c := range pod.Spec.Containers {
					if c.Name == PrometheusContainerName {
						if gomega.Expect(c.Name).To(gomega.Equal("")) {
							return nil
						}
						return errors.New("unexpected Prometheus container injected")
					}
				}
				g.Expect(pod.Spec.InitContainers).To(gomega.BeEmpty())
				return nil
			},
		},
		"DoNotInjectWhenAlreadyInjected": {
			original: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "user-namespace",
					Annotations: map[string]string{
						constants.BatcherInternalAnnotationKey:    "true",
						constants.PrometheusInternalAnnotationKey: "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: PrometheusContainerName,
						},
					},
				},
			},
			expected: func(pod *corev1.Pod) error {
				// 已存在 Prometheus 容器，保持不变
				var count int
				for _, c := range pod.Spec.Containers {
					if c.Name == PrometheusContainerName {
						count++
					}
				}
				g.Expect(count).To(gomega.Equal(1))
				return nil
			},
		},
	}

	cfg := &PrometheusConfig{
		Image:         testPromImage,
		CpuRequest:    testCpuRequest,
		CpuLimit:      testCpuLimit,
		MemoryRequest: testMemoryRequest,
		MemoryLimit:   testMemoryLimit,
	}
	injector := &PrometheusInjector{
		Config:        cfg,
		ConfigContent: testConfigContent,
	}

	for name, scenario := range scenarios {
		// 复制一份 original 防止测试间互相影响
		original := scenario.original.DeepCopy()
		err := injector.InjectPrometheus(original)
		g.Expect(err).To(gomega.BeNil(), "unexpected error in scenario %q", name)
		if diff, _ := kmp.SafeDiff(scenario.original.Spec, original.Spec); diff != "" {
			// 这里使用 expected 回调进一步验证关键字段
			err = scenario.expected(original)
			g.Expect(err).To(gomega.BeNil(), "scenario %q failed: %v", name, diff)
		} else {
			// 对于未修改的 pod 调用 expected 校验（scenario 不修改时 expected 中自行校验为空或不修改的情况）
			err = scenario.expected(original)
			g.Expect(err).To(gomega.BeNil(), "scenario %q failed expected check", name)
		}
	}
}
