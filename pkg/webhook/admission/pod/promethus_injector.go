package pod

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"

	"github.com/kserve/kserve/pkg/constants"
)

const (
	PrometheusConfigMapKeyName = "prometheus"
	PrometheusContainerName    = "prometheus"
	PrometheusDefaultPort      = 9090
	PrometheusConfigVolumeName = "prometheus-config"
	PrometheusConfigMountPath  = "/etc/prometheus"
	PrometheusConfigName       = "prometheus-config"
)

type PrometheusConfig struct {
	Image         string `json:"image"`
	CpuRequest    string `json:"cpuRequest"`
	CpuLimit      string `json:"cpuLimit"`
	MemoryRequest string `json:"memoryRequest"`
	MemoryLimit   string `json:"memoryLimit"`
}

func getPrometheusConfigs(configMap *corev1.ConfigMap) (*PrometheusConfig, error) {
	prometheusConfig := &PrometheusConfig{}
	if prometheusConfigValue, ok := configMap.Data[PrometheusConfigMapKeyName]; ok {
		err := json.Unmarshal([]byte(prometheusConfigValue), &prometheusConfig)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshall prometheus json string due to %w", err)
		}
	}

	// Ensure that we set proper values
	resourceDefaults := []string{
		prometheusConfig.MemoryRequest,
		prometheusConfig.MemoryLimit,
		prometheusConfig.CpuRequest,
		prometheusConfig.CpuLimit,
	}
	for _, key := range resourceDefaults {
		_, err := resource.ParseQuantity(key)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource configuration for %q: %s",
				PrometheusConfigMapKeyName, err.Error())
		}
	}

	return prometheusConfig, nil
}

func getPrometheusConfigContent(configMap *corev1.ConfigMap) (string, error) {
	if prometheusConfigValue, ok := configMap.Data[PrometheusConfigName]; ok {
		return prometheusConfigValue, nil
	}
	return "", fmt.Errorf("prometheus config not found in configmap %s", configMap.Name)
}

func (injector *PrometheusInjector) InjectPrometheus(pod *corev1.Pod) error {
	// 检查是否需要注入 Prometheus 容器(仅在启用batcher和prometheus注释时注入)
	_, injectBatcher := pod.ObjectMeta.Annotations[constants.BatcherInternalAnnotationKey]
	if !injectBatcher {
		return nil
	}
	_, injectPrometheus := pod.ObjectMeta.Annotations[constants.PrometheusInternalAnnotationKey]
	if !injectPrometheus {
		return nil
	}

	// 检查容器是否已经注入
	for _, container := range pod.Spec.Containers {
		if container.Name == PrometheusContainerName {
			return nil
		}
	}

	// 为 Prometheus 配置创建一个EmptyDir卷
	configVolume := corev1.Volume{
		Name: PrometheusConfigVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, configVolume)

	// 添加 init container，在 pod 启动前将配置内容写入到空卷中
	initContainer := corev1.Container{
		Name:  "prometheus-config-init",
		Image: "busybox",
		Command: []string{
			"sh", "-c",
			"echo \"$PROMETHEUS_CONFIG\" > $PROMETHEUS_CONFIG_FILE_PATH",
		},
		Env: []corev1.EnvVar{
			{
				Name:  "PROMETHEUS_CONFIG",
				Value: injector.ConfigContent,
			},
			{
				Name:  "PROMETHEUS_CONFIG_FILE_PATH",
				Value: PrometheusConfigMountPath + "/prometheus.yml",
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      PrometheusConfigVolumeName,
				MountPath: PrometheusConfigMountPath,
			},
		},
	}
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, initContainer)

	// 创建 Prometheus 容器，挂载刚才创建的空卷
	prometheusContainer := &corev1.Container{
		Name:  PrometheusContainerName,
		Image: injector.Config.Image,
		Resources: corev1.ResourceRequirements{
			Limits: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:    resource.MustParse(injector.Config.CpuLimit),
				corev1.ResourceMemory: resource.MustParse(injector.Config.MemoryLimit),
			},
			Requests: map[corev1.ResourceName]resource.Quantity{
				corev1.ResourceCPU:    resource.MustParse(injector.Config.CpuRequest),
				corev1.ResourceMemory: resource.MustParse(injector.Config.MemoryRequest),
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "prometheus-port",
				ContainerPort: PrometheusDefaultPort,
				Protocol:      "TCP",
			},
		},
		// 挂载上面 init container 写入配置文件的 volume
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      PrometheusConfigVolumeName,
				MountPath: PrometheusConfigMountPath,
				ReadOnly:  true,
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Port:   intstr.FromInt(PrometheusDefaultPort),
					Path:   "/-/ready",
					Scheme: "HTTP",
				},
			},
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Port:   intstr.FromInt(PrometheusDefaultPort),
					Path:   "/-/healthy",
					Scheme: "HTTP",
				},
			},
		},
		// 指定 Prometheus 配置文件路径，与 init container 写入的路径一致
		Args: []string{
			"--config.file=/etc/prometheus/prometheus.yml",
		},
	}

	// 添加 Prometheus 容器到 Pod
	pod.Spec.Containers = append(pod.Spec.Containers, *prometheusContainer)

	// 修改 agent 容器（如果存在），增加启动参数及环境变量
	for i, container := range pod.Spec.Containers {
		if container.Name == constants.AgentContainerName {
			container.Args = append(container.Args, "--prometheus-scrape-url=http://localhost:9090/metrics")
			container.Env = append(container.Env, corev1.EnvVar{
				Name:  "PROMETHEUS_SCRAPE_URL",
				Value: "http://localhost:9090/metrics",
			})
			pod.Spec.Containers[i] = container
			break
		}
	}

	klog.Infof("Prometheus container injected into pod %s/%s", pod.Namespace, pod.Name)
	return nil
}

type PrometheusInjector struct {
	Config        *PrometheusConfig
	ConfigContent string // 新增字段，用于存储prometheus配置文件的文本内容
}
