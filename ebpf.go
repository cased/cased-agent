//go:build linux
// +build linux

package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
)

// HTTPEvent matches the C struct
type HTTPEvent struct {
	TimestampNs  uint64
	DurationNs   uint64
	PID          uint32
	TID          uint32
	StatusCode   uint32
	RequestSize  uint32
	ResponseSize uint32
	Method       [8]byte
	Path         [64]byte
	ContainerID  [64]byte
}

// HTTPMetrics aggregates HTTP metrics over a collection interval
type HTTPMetrics struct {
	mu sync.Mutex

	// Per-path metrics
	PathMetrics map[string]*PathMetrics
}

// PathMetrics holds metrics for a specific HTTP path
type PathMetrics struct {
	Method       string
	Path         string
	RequestCount uint64
	ErrorCount   uint64 // 4xx and 5xx
	TotalLatency time.Duration
	Latencies    []time.Duration // For percentile calculation
}

// EBPFCollector manages eBPF programs for HTTP tracing
type EBPFCollector struct {
	enabled bool
	objs    *ebpf.Collection
	links   []link.Link
	reader  *ringbuf.Reader

	metrics *HTTPMetrics
	stopCh  chan struct{}
}

// NewEBPFCollector creates a new eBPF collector
func NewEBPFCollector() (*EBPFCollector, error) {
	// Remove rlimit for eBPF
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("removing memlock: %w", err)
	}

	// Load pre-compiled eBPF program
	// Try multiple paths: /ebpf/ (container), ebpf/ (local dev)
	ebpfPaths := []string{"/ebpf/http_trace.o", "ebpf/http_trace.o"}
	var spec *ebpf.CollectionSpec
	var loadErr error
	for _, path := range ebpfPaths {
		spec, loadErr = ebpf.LoadCollectionSpec(path)
		if loadErr == nil {
			fmt.Printf("Loaded eBPF program from %s\n", path)
			break
		}
	}
	if spec == nil {
		return nil, fmt.Errorf("loading eBPF spec from any path: %w", loadErr)
	}

	objs, err := ebpf.NewCollection(spec)
	if err != nil {
		return nil, fmt.Errorf("creating eBPF collection: %w", err)
	}

	// Attach kprobes
	var links []link.Link

	sendProbe, err := link.Kprobe("__sys_sendto", objs.Programs["trace_send"], nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attaching send kprobe: %w", err)
	}
	links = append(links, sendProbe)

	recvProbe, err := link.Kretprobe("__sys_recvfrom", objs.Programs["trace_recv_exit"], nil)
	if err != nil {
		for _, l := range links {
			l.Close()
		}
		objs.Close()
		return nil, fmt.Errorf("attaching recv kretprobe: %w", err)
	}
	links = append(links, recvProbe)

	// Create ring buffer reader
	reader, err := ringbuf.NewReader(objs.Maps["http_events"])
	if err != nil {
		for _, l := range links {
			l.Close()
		}
		objs.Close()
		return nil, fmt.Errorf("creating ringbuf reader: %w", err)
	}

	return &EBPFCollector{
		enabled: true,
		objs:    objs,
		links:   links,
		reader:  reader,
		metrics: &HTTPMetrics{
			PathMetrics: make(map[string]*PathMetrics),
		},
		stopCh: make(chan struct{}),
	}, nil
}

// Start begins reading eBPF events
func (e *EBPFCollector) Start() {
	go e.readEvents()
}

// Stop stops the eBPF collector
func (e *EBPFCollector) Stop() {
	close(e.stopCh)
	e.reader.Close()
	for _, l := range e.links {
		l.Close()
	}
	e.objs.Close()
}

func (e *EBPFCollector) readEvents() {
	for {
		select {
		case <-e.stopCh:
			return
		default:
		}

		record, err := e.reader.Read()
		if err != nil {
			if errors.Is(err, ringbuf.ErrClosed) {
				return
			}
			continue
		}

		var event HTTPEvent
		if err := binary.Read(bytes.NewReader(record.RawSample), binary.LittleEndian, &event); err != nil {
			continue
		}

		e.processEvent(&event)
	}
}

func (e *EBPFCollector) processEvent(event *HTTPEvent) {
	method := string(bytes.TrimRight(event.Method[:], "\x00"))
	path := string(bytes.TrimRight(event.Path[:], "\x00"))
	duration := time.Duration(event.DurationNs)

	key := method + " " + path

	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	pm, ok := e.metrics.PathMetrics[key]
	if !ok {
		pm = &PathMetrics{
			Method:    method,
			Path:      path,
			Latencies: make([]time.Duration, 0, 1000),
		}
		e.metrics.PathMetrics[key] = pm
	}

	pm.RequestCount++
	pm.TotalLatency += duration
	pm.Latencies = append(pm.Latencies, duration)

	// Count errors (4xx and 5xx)
	if event.StatusCode >= 400 {
		pm.ErrorCount++
	}

	// Keep latency slice bounded
	if len(pm.Latencies) > 10000 {
		pm.Latencies = pm.Latencies[len(pm.Latencies)-5000:]
	}
}

// CollectMetrics returns metrics and resets counters
func (e *EBPFCollector) CollectMetrics(timestamp int64, clusterID, nodeName string) []Metric {
	e.metrics.mu.Lock()
	defer e.metrics.mu.Unlock()

	var metrics []Metric

	for key, pm := range e.metrics.PathMetrics {
		if pm.RequestCount == 0 {
			continue
		}

		tags := map[string]string{
			"method": pm.Method,
			"path":   pm.Path,
			"node":   nodeName,
		}

		// Request rate
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "http.request_count",
			Value:      float64(pm.RequestCount),
			Unit:       "count",
			Tags:       tags,
			ClusterID:  clusterID,
			NodeName:   nodeName,
		})

		// Error rate
		if pm.RequestCount > 0 {
			errorRate := float64(pm.ErrorCount) / float64(pm.RequestCount) * 100
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "http.error_rate",
				Value:      errorRate,
				Unit:       "percent",
				Tags:       tags,
				ClusterID:  clusterID,
				NodeName:   nodeName,
			})
		}

		// Average latency
		if pm.RequestCount > 0 {
			avgLatency := pm.TotalLatency.Milliseconds() / int64(pm.RequestCount)
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "http.latency_avg",
				Value:      float64(avgLatency),
				Unit:       "ms",
				Tags:       tags,
				ClusterID:  clusterID,
				NodeName:   nodeName,
			})
		}

		// Percentile latencies
		if len(pm.Latencies) > 0 {
			p50, p95, p99 := calculatePercentiles(pm.Latencies)
			metrics = append(metrics,
				Metric{
					Timestamp:  timestamp,
					MetricName: "http.latency_p50",
					Value:      float64(p50.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  clusterID,
					NodeName:   nodeName,
				},
				Metric{
					Timestamp:  timestamp,
					MetricName: "http.latency_p95",
					Value:      float64(p95.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  clusterID,
					NodeName:   nodeName,
				},
				Metric{
					Timestamp:  timestamp,
					MetricName: "http.latency_p99",
					Value:      float64(p99.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  clusterID,
					NodeName:   nodeName,
				},
			)
		}

		// Reset for next interval
		delete(e.metrics.PathMetrics, key)
	}

	return metrics
}

func calculatePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	n := len(latencies)
	if n == 0 {
		return 0, 0, 0
	}

	// Sort for percentile calculation
	sorted := make([]time.Duration, n)
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	if n*99/100 < n {
		p99 = sorted[n*99/100]
	} else {
		p99 = sorted[n-1]
	}

	return p50, p95, p99
}

// IsEnabled returns whether eBPF is enabled
func (e *EBPFCollector) IsEnabled() bool {
	return e.enabled
}
