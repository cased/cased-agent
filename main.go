// cased-agent: Kubernetes metrics collector for Cased observability
//
// Runs as a DaemonSet, collects container/pod metrics from cgroups and /proc,
// enriches with Kubernetes metadata, and sends to Cased API.
//
// Similar to Groundcover's approach but Phase 1 (no eBPF yet):
// - Resource metrics: CPU, memory, network, disk I/O
// - Kubernetes metadata: pod, namespace, node, labels
// - Golden signals computed server-side from these metrics

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Config holds agent configuration
type Config struct {
	APIEndpoint     string
	APIKey          string
	ClusterID       string
	NodeName        string
	CollectInterval time.Duration
	BatchSize       int
	ProcPath        string // Path to /proc (or /host/proc in container)
	CgroupPath      string // Path to cgroups
	EnableEBPF      bool   // Enable eBPF HTTP tracing
	EnableOTel      bool   // Enable OpenTelemetry receiver
	OTelPort        int    // Port for OTLP HTTP receiver
}

// Metric represents a single metric data point
type Metric struct {
	Timestamp     int64             `json:"timestamp"`
	MetricName    string            `json:"metric_name"`
	Value         float64           `json:"value"`
	Unit          string            `json:"unit"`
	Tags          map[string]string `json:"tags"`
	ClusterID     string            `json:"cluster_id"`
	NodeName      string            `json:"node_name"`
	Namespace     string            `json:"namespace,omitempty"`
	PodName       string            `json:"pod_name,omitempty"`
	ContainerName string            `json:"container_name,omitempty"`
	PodUID        string            `json:"pod_uid,omitempty"`
}

// MetricsBatch is sent to the API
type MetricsBatch struct {
	Metrics   []Metric `json:"metrics"`
	ClusterID string   `json:"cluster_id"`
}

// ContainerStats holds raw stats for a container
type ContainerStats struct {
	Namespace     string
	PodName       string
	PodUID        string
	ContainerName string
	ContainerID   string
	Labels        map[string]string

	// CPU stats (from cpu.stat or cpuacct)
	CPUUsageNanos     uint64
	CPUUserNanos      uint64
	CPUSystemNanos    uint64
	CPUThrottledNanos uint64
	CPUPeriods        uint64
	CPUThrottled      uint64

	// Memory stats (from memory.stat or memory.current)
	MemoryUsageBytes uint64
	MemoryLimitBytes uint64
	MemoryRSSBytes   uint64
	MemoryCacheBytes uint64
	MemorySwapBytes  uint64

	// Network stats (from /proc/net/dev in container namespace)
	NetworkRxBytes   uint64
	NetworkTxBytes   uint64
	NetworkRxPackets uint64
	NetworkTxPackets uint64
	NetworkRxErrors  uint64
	NetworkTxErrors  uint64

	// Disk I/O stats (from io.stat or blkio)
	DiskReadBytes  uint64
	DiskWriteBytes uint64
	DiskReadOps    uint64
	DiskWriteOps   uint64
}

// Agent collects and sends metrics
type Agent struct {
	config    *Config
	k8sClient *kubernetes.Clientset
	httpClient *http.Client

	// Previous stats for rate calculations
	prevStats     map[string]*ContainerStats
	prevStatsTime time.Time

	// Additional collectors
	ebpfCollector  *EBPFCollector
	k8sEvents      *K8sEventCollector
	otelReceiver   *OTelReceiver

	// Health status
	ready bool
}

func main() {
	// Parse flags
	endpoint := flag.String("endpoint", getEnv("CASED_API_ENDPOINT", "https://api.cased.com"), "Cased API endpoint")
	apiKey := flag.String("api-key", getEnv("CASED_API_KEY", ""), "Cased API key")
	clusterID := flag.String("cluster-id", getEnv("CASED_CLUSTER_ID", ""), "Cluster identifier")
	nodeName := flag.String("node-name", getEnv("NODE_NAME", ""), "Node name (usually from downward API)")
	interval := flag.Duration("interval", 15*time.Second, "Collection interval")
	batchSize := flag.Int("batch-size", 100, "Max metrics per batch")
	procPath := flag.String("proc-path", getEnv("PROC_PATH", "/proc"), "Path to /proc filesystem")
	cgroupPath := flag.String("cgroup-path", getEnv("CGROUP_PATH", "/sys/fs/cgroup"), "Path to cgroup filesystem")
	enableEBPF := flag.Bool("enable-ebpf", getEnv("ENABLE_EBPF", "false") == "true", "Enable eBPF HTTP tracing")
	enableOTel := flag.Bool("enable-otel", getEnv("ENABLE_OTEL", "false") == "true", "Enable OpenTelemetry receiver")
	otelPort := flag.Int("otel-port", 4318, "Port for OTLP HTTP receiver")
	flag.Parse()

	if *apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: CASED_API_KEY or --api-key required")
		os.Exit(1)
	}
	if *clusterID == "" {
		fmt.Fprintln(os.Stderr, "Error: CASED_CLUSTER_ID or --cluster-id required")
		os.Exit(1)
	}

	config := &Config{
		APIEndpoint:     *endpoint,
		APIKey:          *apiKey,
		ClusterID:       *clusterID,
		NodeName:        *nodeName,
		CollectInterval: *interval,
		BatchSize:       *batchSize,
		ProcPath:        *procPath,
		CgroupPath:      *cgroupPath,
		EnableEBPF:      *enableEBPF,
		EnableOTel:      *enableOTel,
		OTelPort:        *otelPort,
	}

	agent, err := NewAgent(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating agent: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("cased-agent starting: cluster=%s node=%s interval=%s\n",
		config.ClusterID, config.NodeName, config.CollectInterval)

	agent.Run(context.Background())
}

// NewAgent creates a new metrics agent
func NewAgent(config *Config) (*Agent, error) {
	// Create in-cluster Kubernetes client
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to local development mode
		fmt.Printf("Warning: not running in cluster, k8s metadata disabled: %v\n", err)
	}

	var k8sClient *kubernetes.Clientset
	if k8sConfig != nil {
		k8sClient, err = kubernetes.NewForConfig(k8sConfig)
		if err != nil {
			return nil, fmt.Errorf("creating k8s client: %w", err)
		}
	}

	agent := &Agent{
		config:    config,
		k8sClient: k8sClient,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		prevStats: make(map[string]*ContainerStats),
	}

	// Initialize K8s event collector
	if k8sClient != nil {
		agent.k8sEvents = NewK8sEventCollector(k8sClient, config.NodeName, config.ClusterID)
	}

	// Initialize eBPF collector (optional)
	if config.EnableEBPF {
		ebpf, err := NewEBPFCollector()
		if err != nil {
			fmt.Printf("Warning: eBPF initialization failed (non-fatal): %v\n", err)
		} else {
			agent.ebpfCollector = ebpf
			fmt.Println("eBPF HTTP tracing enabled")
		}
	}

	// Initialize OpenTelemetry receiver (optional)
	if config.EnableOTel {
		agent.otelReceiver = NewOTelReceiver(config.OTelPort, config.ClusterID, config.NodeName)
		fmt.Printf("OpenTelemetry receiver will listen on port %d\n", config.OTelPort)
	}

	return agent, nil
}

// startHealthServer starts the HTTP health check server
func (a *Agent) startHealthServer() {
	mux := http.NewServeMux()

	// Liveness probe - always healthy if process is running
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Readiness probe - healthy once we've successfully collected metrics
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if a.ready {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
		}
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		fmt.Println("Health server listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "Health server error: %v\n", err)
		}
	}()
}

// Run starts the collection loop
func (a *Agent) Run(ctx context.Context) {
	// Start health server first
	a.startHealthServer()

	ticker := time.NewTicker(a.config.CollectInterval)
	defer ticker.Stop()

	// Start additional collectors
	if a.k8sEvents != nil {
		a.k8sEvents.Start(ctx)
	}
	if a.ebpfCollector != nil {
		a.ebpfCollector.Start()
	}
	if a.otelReceiver != nil {
		if err := a.otelReceiver.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: OTel receiver failed to start: %v\n", err)
		}
	}

	// Collect immediately on start
	a.collectAndSend(ctx)

	// Mark as ready after first successful collection
	a.ready = true

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Agent shutting down")
			a.shutdown()
			return
		case <-ticker.C:
			a.collectAndSend(ctx)
		}
	}
}

func (a *Agent) shutdown() {
	if a.k8sEvents != nil {
		a.k8sEvents.Stop()
	}
	if a.ebpfCollector != nil {
		a.ebpfCollector.Stop()
	}
	if a.otelReceiver != nil {
		a.otelReceiver.Stop()
	}
}

func (a *Agent) collectAndSend(ctx context.Context) {
	start := time.Now()

	metrics, err := a.collectMetrics(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error collecting metrics: %v\n", err)
		return
	}

	if len(metrics) == 0 {
		fmt.Println("No metrics collected")
		return
	}

	// Send in batches
	for i := 0; i < len(metrics); i += a.config.BatchSize {
		end := i + a.config.BatchSize
		if end > len(metrics) {
			end = len(metrics)
		}

		batch := MetricsBatch{
			Metrics:   metrics[i:end],
			ClusterID: a.config.ClusterID,
		}

		if err := a.sendBatch(ctx, &batch); err != nil {
			fmt.Fprintf(os.Stderr, "Error sending batch: %v\n", err)
		}
	}

	// Send OTel spans if receiver is enabled
	if a.otelReceiver != nil {
		spans := a.otelReceiver.CollectSpans()
		if len(spans) > 0 {
			if err := a.sendSpans(ctx, spans); err != nil {
				fmt.Fprintf(os.Stderr, "Error sending spans: %v\n", err)
			} else {
				fmt.Printf("Sent %d OTel spans\n", len(spans))
			}
		}
	}

	fmt.Printf("Collected and sent %d metrics in %s\n", len(metrics), time.Since(start))
}

func (a *Agent) collectMetrics(ctx context.Context) ([]Metric, error) {
	var metrics []Metric
	now := time.Now()
	timestamp := now.UnixMilli()

	// Collect node-level metrics
	nodeMetrics, err := a.collectNodeMetrics(timestamp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: node metrics collection failed: %v\n", err)
	} else {
		metrics = append(metrics, nodeMetrics...)
	}

	// Collect container metrics
	containerStats, err := a.collectContainerStats()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: container metrics collection failed: %v\n", err)
	} else {
		// Enrich with K8s metadata (pod name, namespace, labels)
		a.enrichWithK8sMetadata(ctx, containerStats)
		containerMetrics := a.statsToMetrics(containerStats, timestamp, now)
		metrics = append(metrics, containerMetrics...)
	}

	// Collect K8s events as metrics
	if a.k8sEvents != nil {
		eventMetrics := a.k8sEvents.CollectMetrics(timestamp)
		metrics = append(metrics, eventMetrics...)
	}

	// Collect eBPF HTTP metrics
	if a.ebpfCollector != nil && a.ebpfCollector.IsEnabled() {
		httpMetrics := a.ebpfCollector.CollectMetrics(timestamp, a.config.ClusterID, a.config.NodeName)
		metrics = append(metrics, httpMetrics...)
	}

	// Collect OpenTelemetry trace metrics
	if a.otelReceiver != nil {
		traceMetrics := a.otelReceiver.CollectMetrics(timestamp)
		metrics = append(metrics, traceMetrics...)
	}

	return metrics, nil
}

// collectNodeMetrics collects node-level CPU, memory, network stats
func (a *Agent) collectNodeMetrics(timestamp int64) ([]Metric, error) {
	var metrics []Metric
	tags := map[string]string{
		"node": a.config.NodeName,
	}

	// CPU from /proc/stat
	cpuMetrics, err := a.readNodeCPU()
	if err == nil {
		for name, value := range cpuMetrics {
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "node.cpu." + name,
				Value:      value,
				Unit:       "percent",
				Tags:       tags,
				ClusterID:  a.config.ClusterID,
				NodeName:   a.config.NodeName,
			})
		}
	}

	// Memory from /proc/meminfo
	memMetrics, err := a.readNodeMemory()
	if err == nil {
		for name, value := range memMetrics {
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "node.memory." + name,
				Value:      value,
				Unit:       "bytes",
				Tags:       tags,
				ClusterID:  a.config.ClusterID,
				NodeName:   a.config.NodeName,
			})
		}
	}

	// Network from /proc/net/dev
	netMetrics, err := a.readNodeNetwork()
	if err == nil {
		for name, value := range netMetrics {
			unit := "bytes"
			if strings.Contains(name, "packets") || strings.Contains(name, "errors") {
				unit = "count"
			}
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "node.network." + name,
				Value:      value,
				Unit:       unit,
				Tags:       tags,
				ClusterID:  a.config.ClusterID,
				NodeName:   a.config.NodeName,
			})
		}
	}

	return metrics, nil
}

func (a *Agent) readNodeCPU() (map[string]float64, error) {
	data, err := os.ReadFile(filepath.Join(a.config.ProcPath, "stat"))
	if err != nil {
		return nil, err
	}

	// Parse first line: cpu user nice system idle iowait irq softirq steal guest guest_nice
	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty /proc/stat")
	}

	fields := strings.Fields(lines[0])
	if len(fields) < 5 || fields[0] != "cpu" {
		return nil, fmt.Errorf("unexpected /proc/stat format")
	}

	user, _ := strconv.ParseFloat(fields[1], 64)
	nice, _ := strconv.ParseFloat(fields[2], 64)
	system, _ := strconv.ParseFloat(fields[3], 64)
	idle, _ := strconv.ParseFloat(fields[4], 64)
	iowait := 0.0
	if len(fields) > 5 {
		iowait, _ = strconv.ParseFloat(fields[5], 64)
	}

	total := user + nice + system + idle + iowait

	return map[string]float64{
		"user_percent":   (user / total) * 100,
		"system_percent": (system / total) * 100,
		"idle_percent":   (idle / total) * 100,
		"iowait_percent": (iowait / total) * 100,
	}, nil
}

func (a *Agent) readNodeMemory() (map[string]float64, error) {
	data, err := os.ReadFile(filepath.Join(a.config.ProcPath, "meminfo"))
	if err != nil {
		return nil, err
	}

	result := make(map[string]float64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		key := strings.TrimSuffix(fields[0], ":")
		value, _ := strconv.ParseFloat(fields[1], 64)

		// Convert kB to bytes
		if len(fields) >= 3 && fields[2] == "kB" {
			value *= 1024
		}

		switch key {
		case "MemTotal":
			result["total"] = value
		case "MemFree":
			result["free"] = value
		case "MemAvailable":
			result["available"] = value
		case "Buffers":
			result["buffers"] = value
		case "Cached":
			result["cached"] = value
		case "SwapTotal":
			result["swap_total"] = value
		case "SwapFree":
			result["swap_free"] = value
		}
	}

	// Calculate used
	if total, ok := result["total"]; ok {
		if avail, ok := result["available"]; ok {
			result["used"] = total - avail
			result["used_percent"] = ((total - avail) / total) * 100
		}
	}

	return result, nil
}

func (a *Agent) readNodeNetwork() (map[string]float64, error) {
	data, err := os.ReadFile(filepath.Join(a.config.ProcPath, "net/dev"))
	if err != nil {
		return nil, err
	}

	result := map[string]float64{
		"rx_bytes":   0,
		"tx_bytes":   0,
		"rx_packets": 0,
		"tx_packets": 0,
		"rx_errors":  0,
		"tx_errors":  0,
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines[2:] { // Skip header lines
		fields := strings.Fields(line)
		if len(fields) < 17 {
			continue
		}

		iface := strings.TrimSuffix(fields[0], ":")
		// Skip loopback and virtual interfaces
		if iface == "lo" || strings.HasPrefix(iface, "veth") || strings.HasPrefix(iface, "docker") {
			continue
		}

		rxBytes, _ := strconv.ParseFloat(fields[1], 64)
		rxPackets, _ := strconv.ParseFloat(fields[2], 64)
		rxErrors, _ := strconv.ParseFloat(fields[3], 64)
		txBytes, _ := strconv.ParseFloat(fields[9], 64)
		txPackets, _ := strconv.ParseFloat(fields[10], 64)
		txErrors, _ := strconv.ParseFloat(fields[11], 64)

		result["rx_bytes"] += rxBytes
		result["tx_bytes"] += txBytes
		result["rx_packets"] += rxPackets
		result["tx_packets"] += txPackets
		result["rx_errors"] += rxErrors
		result["tx_errors"] += txErrors
	}

	return result, nil
}

// collectContainerStats reads cgroup stats for containers
func (a *Agent) collectContainerStats() ([]*ContainerStats, error) {
	cgroupPath := a.config.CgroupPath

	// Try cgroup v2 first
	if _, err := os.Stat(filepath.Join(cgroupPath, "cgroup.controllers")); err == nil {
		return a.collectCgroupV2Stats(cgroupPath)
	}

	// Fall back to cgroup v1
	return a.collectCgroupV1Stats(cgroupPath)
}

func (a *Agent) collectCgroupV2Stats(basePath string) ([]*ContainerStats, error) {
	var stats []*ContainerStats

	// Paths to search for containers (kubernetes and docker compose)
	searchPaths := []string{
		filepath.Join(basePath, "kubepods.slice"),
		filepath.Join(basePath, "kubepods"),
		filepath.Join(basePath, "docker"),                    // Docker containers
		filepath.Join(basePath, "system.slice"),              // systemd services
	}

	for _, searchPath := range searchPaths {
		if _, err := os.Stat(searchPath); err != nil {
			continue
		}

		err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
			if err != nil || !info.IsDir() {
				return nil
			}

			// Look for container directories
			// Kubernetes: cri-containerd-*, docker-*
			// Docker Compose: docker/<container-id>
			basename := filepath.Base(path)
			isContainer := strings.HasPrefix(basename, "cri-containerd-") ||
				strings.HasPrefix(basename, "docker-") ||
				(strings.Contains(path, "/docker/") && len(basename) == 64) // Docker container ID

			if !isContainer {
				return nil
			}

			stat, err := a.readCgroupV2Stats(path)
			if err != nil {
				return nil
			}

			// Extract pod/container info from path
			a.enrichFromPath(stat, path)
			stats = append(stats, stat)
			return nil
		})
		if err != nil {
			continue
		}
	}

	return stats, nil
}

func (a *Agent) readCgroupV2Stats(cgroupPath string) (*ContainerStats, error) {
	stat := &ContainerStats{}

	// CPU stats from cpu.stat
	if data, err := os.ReadFile(filepath.Join(cgroupPath, "cpu.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "usage_usec":
				stat.CPUUsageNanos = value * 1000
			case "user_usec":
				stat.CPUUserNanos = value * 1000
			case "system_usec":
				stat.CPUSystemNanos = value * 1000
			case "throttled_usec":
				stat.CPUThrottledNanos = value * 1000
			case "nr_periods":
				stat.CPUPeriods = value
			case "nr_throttled":
				stat.CPUThrottled = value
			}
		}
	}

	// Memory from memory.current and memory.stat
	if data, err := os.ReadFile(filepath.Join(cgroupPath, "memory.current")); err == nil {
		stat.MemoryUsageBytes, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	}
	if data, err := os.ReadFile(filepath.Join(cgroupPath, "memory.max")); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "max" {
			stat.MemoryLimitBytes, _ = strconv.ParseUint(s, 10, 64)
		}
	}
	if data, err := os.ReadFile(filepath.Join(cgroupPath, "memory.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			switch fields[0] {
			case "anon":
				stat.MemoryRSSBytes = value
			case "file":
				stat.MemoryCacheBytes = value
			case "swap":
				stat.MemorySwapBytes = value
			}
		}
	}

	// I/O from io.stat
	if data, err := os.ReadFile(filepath.Join(cgroupPath, "io.stat")); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			for _, field := range fields[1:] {
				parts := strings.Split(field, "=")
				if len(parts) != 2 {
					continue
				}
				value, _ := strconv.ParseUint(parts[1], 10, 64)
				switch parts[0] {
				case "rbytes":
					stat.DiskReadBytes += value
				case "wbytes":
					stat.DiskWriteBytes += value
				case "rios":
					stat.DiskReadOps += value
				case "wios":
					stat.DiskWriteOps += value
				}
			}
		}
	}

	return stat, nil
}

func (a *Agent) collectCgroupV1Stats(basePath string) ([]*ContainerStats, error) {
	var stats []*ContainerStats

	cpuPath := filepath.Join(basePath, "cpu", "kubepods")
	_ = filepath.Join(basePath, "memory", "kubepods") // Used for memory path substitution

	if _, err := os.Stat(cpuPath); err != nil {
		return stats, nil
	}

	err := filepath.Walk(cpuPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || !info.IsDir() {
			return nil
		}

		// Look for container directories
		if !strings.Contains(path, "docker") && !strings.Contains(path, "cri-containerd") {
			return nil
		}

		stat := &ContainerStats{}
		a.enrichFromPath(stat, path)

		// CPU stats
		if data, err := os.ReadFile(filepath.Join(path, "cpuacct.usage")); err == nil {
			stat.CPUUsageNanos, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		}

		// Memory stats - find corresponding memory cgroup
		memContainerPath := strings.Replace(path, "/cpu/", "/memory/", 1)
		if data, err := os.ReadFile(filepath.Join(memContainerPath, "memory.usage_in_bytes")); err == nil {
			stat.MemoryUsageBytes, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		}
		if data, err := os.ReadFile(filepath.Join(memContainerPath, "memory.limit_in_bytes")); err == nil {
			stat.MemoryLimitBytes, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		}

		stats = append(stats, stat)
		return nil
	})

	return stats, err
}

func (a *Agent) enrichFromPath(stat *ContainerStats, path string) {
	// Extract pod UID and container ID from cgroup path
	// Typical paths:
	// cgroup v2: /sys/fs/cgroup/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod<uid>.slice/cri-containerd-<containerid>.scope
	// cgroup v1: /sys/fs/cgroup/cpu/kubepods/burstable/pod<uid>/<containerid>

	parts := strings.Split(path, "/")
	for _, part := range parts {
		if strings.HasPrefix(part, "pod") || strings.Contains(part, "-pod") {
			// Extract pod UID
			uid := part
			uid = strings.TrimPrefix(uid, "pod")
			uid = strings.TrimPrefix(uid, "kubepods-burstable-pod")
			uid = strings.TrimPrefix(uid, "kubepods-besteffort-pod")
			uid = strings.TrimPrefix(uid, "kubepods-guaranteed-pod")
			uid = strings.TrimSuffix(uid, ".slice")
			// Convert underscores back to dashes (cgroup escaping)
			uid = strings.ReplaceAll(uid, "_", "-")
			stat.PodUID = uid
		}
		if strings.HasPrefix(part, "cri-containerd-") || strings.HasPrefix(part, "docker-") {
			id := part
			id = strings.TrimPrefix(id, "cri-containerd-")
			id = strings.TrimPrefix(id, "docker-")
			id = strings.TrimSuffix(id, ".scope")
			stat.ContainerID = id
		}
	}
}

func (a *Agent) statsToMetrics(stats []*ContainerStats, timestamp int64, now time.Time) []Metric {
	var metrics []Metric
	interval := now.Sub(a.prevStatsTime).Seconds()
	if interval <= 0 {
		interval = 1
	}

	// Build set of current container keys for cleanup
	currentKeys := make(map[string]bool)

	for _, stat := range stats {
		key := stat.PodUID + "/" + stat.ContainerID
		currentKeys[key] = true

		tags := map[string]string{
			"node":      a.config.NodeName,
			"pod_uid":   stat.PodUID,
			"container": stat.ContainerName,
			"namespace": stat.Namespace,
			"pod":       stat.PodName,
		}

		// Memory metrics (absolute values)
		metrics = append(metrics, Metric{
			Timestamp:     timestamp,
			MetricName:    "container.memory.usage",
			Value:         float64(stat.MemoryUsageBytes),
			Unit:          "bytes",
			Tags:          tags,
			ClusterID:     a.config.ClusterID,
			NodeName:      a.config.NodeName,
			Namespace:     stat.Namespace,
			PodName:       stat.PodName,
			ContainerName: stat.ContainerName,
			PodUID:        stat.PodUID,
		})

		if stat.MemoryLimitBytes > 0 {
			metrics = append(metrics, Metric{
				Timestamp:     timestamp,
				MetricName:    "container.memory.limit",
				Value:         float64(stat.MemoryLimitBytes),
				Unit:          "bytes",
				Tags:          tags,
				ClusterID:     a.config.ClusterID,
				NodeName:      a.config.NodeName,
				Namespace:     stat.Namespace,
				PodName:       stat.PodName,
				ContainerName: stat.ContainerName,
				PodUID:        stat.PodUID,
			})

			usagePercent := float64(stat.MemoryUsageBytes) / float64(stat.MemoryLimitBytes) * 100
			metrics = append(metrics, Metric{
				Timestamp:     timestamp,
				MetricName:    "container.memory.usage_percent",
				Value:         usagePercent,
				Unit:          "percent",
				Tags:          tags,
				ClusterID:     a.config.ClusterID,
				NodeName:      a.config.NodeName,
				Namespace:     stat.Namespace,
				PodName:       stat.PodName,
				ContainerName: stat.ContainerName,
				PodUID:        stat.PodUID,
			})
		}

		// Memory breakdown metrics (RSS vs cache)
		if stat.MemoryRSSBytes > 0 {
			metrics = append(metrics, Metric{
				Timestamp:     timestamp,
				MetricName:    "container.memory.rss",
				Value:         float64(stat.MemoryRSSBytes),
				Unit:          "bytes",
				Tags:          tags,
				ClusterID:     a.config.ClusterID,
				NodeName:      a.config.NodeName,
				Namespace:     stat.Namespace,
				PodName:       stat.PodName,
				ContainerName: stat.ContainerName,
				PodUID:        stat.PodUID,
			})
		}
		if stat.MemoryCacheBytes > 0 {
			metrics = append(metrics, Metric{
				Timestamp:     timestamp,
				MetricName:    "container.memory.cache",
				Value:         float64(stat.MemoryCacheBytes),
				Unit:          "bytes",
				Tags:          tags,
				ClusterID:     a.config.ClusterID,
				NodeName:      a.config.NodeName,
				Namespace:     stat.Namespace,
				PodName:       stat.PodName,
				ContainerName: stat.ContainerName,
				PodUID:        stat.PodUID,
			})
		}
		if stat.MemorySwapBytes > 0 {
			metrics = append(metrics, Metric{
				Timestamp:     timestamp,
				MetricName:    "container.memory.swap",
				Value:         float64(stat.MemorySwapBytes),
				Unit:          "bytes",
				Tags:          tags,
				ClusterID:     a.config.ClusterID,
				NodeName:      a.config.NodeName,
				Namespace:     stat.Namespace,
				PodName:       stat.PodName,
				ContainerName: stat.ContainerName,
				PodUID:        stat.PodUID,
			})
		}

		// CPU metrics (rate - need previous value)
		if prev, ok := a.prevStats[key]; ok && interval > 0 {
			// Only calculate if we have valid previous data (prevent huge deltas on first read)
			if prev.CPUUsageNanos > 0 && stat.CPUUsageNanos > prev.CPUUsageNanos {
				cpuDelta := float64(stat.CPUUsageNanos - prev.CPUUsageNanos)
				cpuPercent := (cpuDelta / (interval * 1e9)) * 100 // nanoseconds to percent

				// Sanity check: cap at 10000% (100 cores max)
				if cpuPercent >= 0 && cpuPercent <= 10000 {
					metrics = append(metrics, Metric{
						Timestamp:     timestamp,
						MetricName:    "container.cpu.usage_percent",
						Value:         cpuPercent,
						Unit:          "percent",
						Tags:          tags,
						ClusterID:     a.config.ClusterID,
						NodeName:      a.config.NodeName,
						Namespace:     stat.Namespace,
						PodName:       stat.PodName,
						ContainerName: stat.ContainerName,
						PodUID:        stat.PodUID,
					})
				}
			}

			// CPU throttling metrics
			if stat.CPUPeriods > prev.CPUPeriods {
				throttledPeriods := stat.CPUThrottled - prev.CPUThrottled
				totalPeriods := stat.CPUPeriods - prev.CPUPeriods
				throttlePercent := float64(throttledPeriods) / float64(totalPeriods) * 100

				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.cpu.throttle_percent",
					Value:         throttlePercent,
					Unit:          "percent",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})

				throttledTimeDelta := float64(stat.CPUThrottledNanos - prev.CPUThrottledNanos)
				throttledTimeMs := throttledTimeDelta / 1e6 / interval // ms per second

				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.cpu.throttled_time",
					Value:         throttledTimeMs,
					Unit:          "ms/sec",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})
			}

			// Network rates
			if stat.NetworkRxBytes > prev.NetworkRxBytes {
				rxRate := float64(stat.NetworkRxBytes-prev.NetworkRxBytes) / interval
				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.network.rx_bytes_per_sec",
					Value:         rxRate,
					Unit:          "bytes/sec",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})
			}
			if stat.NetworkTxBytes > prev.NetworkTxBytes {
				txRate := float64(stat.NetworkTxBytes-prev.NetworkTxBytes) / interval
				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.network.tx_bytes_per_sec",
					Value:         txRate,
					Unit:          "bytes/sec",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})
			}

			// Disk I/O rates
			if stat.DiskReadBytes > prev.DiskReadBytes {
				readRate := float64(stat.DiskReadBytes-prev.DiskReadBytes) / interval
				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.disk.read_bytes_per_sec",
					Value:         readRate,
					Unit:          "bytes/sec",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})
			}
			if stat.DiskWriteBytes > prev.DiskWriteBytes {
				writeRate := float64(stat.DiskWriteBytes-prev.DiskWriteBytes) / interval
				metrics = append(metrics, Metric{
					Timestamp:     timestamp,
					MetricName:    "container.disk.write_bytes_per_sec",
					Value:         writeRate,
					Unit:          "bytes/sec",
					Tags:          tags,
					ClusterID:     a.config.ClusterID,
					NodeName:      a.config.NodeName,
					Namespace:     stat.Namespace,
					PodName:       stat.PodName,
					ContainerName: stat.ContainerName,
					PodUID:        stat.PodUID,
				})
			}
		}

		// Store for next iteration
		a.prevStats[key] = stat
	}

	// Clean up stale entries - remove containers that no longer exist
	for key := range a.prevStats {
		if !currentKeys[key] {
			delete(a.prevStats, key)
		}
	}

	a.prevStatsTime = now
	return metrics
}

// enrichWithK8sMetadata looks up pod info from Kubernetes API
func (a *Agent) enrichWithK8sMetadata(ctx context.Context, stats []*ContainerStats) {
	if a.k8sClient == nil {
		return
	}

	// List pods on this node
	pods, err := a.k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + a.config.NodeName,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to list pods: %v\n", err)
		return
	}

	// Build lookup map by pod UID
	podsByUID := make(map[string]*struct {
		namespace string
		name      string
		labels    map[string]string
	})
	for _, pod := range pods.Items {
		podsByUID[string(pod.UID)] = &struct {
			namespace string
			name      string
			labels    map[string]string
		}{
			namespace: pod.Namespace,
			name:      pod.Name,
			labels:    pod.Labels,
		}
	}

	// Enrich stats
	for _, stat := range stats {
		if pod, ok := podsByUID[stat.PodUID]; ok {
			stat.Namespace = pod.namespace
			stat.PodName = pod.name
			stat.Labels = pod.labels
		}
	}
}

func (a *Agent) sendBatch(ctx context.Context, batch *MetricsBatch) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshaling batch: %w", err)
	}

	url := a.config.APIEndpoint + "/api/v1/telemetry/metrics"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.config.APIKey)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// SpansBatch is sent to the API
type SpansBatch struct {
	Spans []StoredSpan `json:"spans"`
}

func (a *Agent) sendSpans(ctx context.Context, spans []StoredSpan) error {
	batch := SpansBatch{Spans: spans}
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshaling spans: %w", err)
	}

	url := a.config.APIEndpoint + "/api/v1/telemetry/spans"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+a.config.APIKey)

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending spans request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
