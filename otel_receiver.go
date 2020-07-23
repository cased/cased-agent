package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

// OTelSpan represents an OpenTelemetry span
type OTelSpan struct {
	TraceID      string            `json:"traceId"`
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId,omitempty"`
	Name         string            `json:"name"`
	Kind         int               `json:"kind"`
	StartTimeNs  uint64            `json:"startTimeUnixNano"`
	EndTimeNs    uint64            `json:"endTimeUnixNano"`
	Attributes   map[string]any    `json:"attributes"`
	Status       *OTelStatus       `json:"status,omitempty"`
	Events       []OTelSpanEvent   `json:"events,omitempty"`
}

// OTelStatus represents span status
type OTelStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// OTelSpanEvent represents a span event
type OTelSpanEvent struct {
	Name       string         `json:"name"`
	TimeNs     uint64         `json:"timeUnixNano"`
	Attributes map[string]any `json:"attributes"`
}

// OTelTraceRequest is the incoming OTLP trace request
type OTelTraceRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

// ResourceSpans groups spans by resource
type ResourceSpans struct {
	Resource   *OTelResource  `json:"resource,omitempty"`
	ScopeSpans []ScopeSpans   `json:"scopeSpans"`
}

// OTelResource contains resource attributes
type OTelResource struct {
	Attributes map[string]any `json:"attributes"`
}

// ScopeSpans groups spans by instrumentation scope
type ScopeSpans struct {
	Scope *InstrumentationScope `json:"scope,omitempty"`
	Spans []OTelSpan            `json:"spans"`
}

// InstrumentationScope identifies the instrumentation library
type InstrumentationScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// TraceMetrics aggregates trace metrics
type TraceMetrics struct {
	mu sync.Mutex

	// Per-service metrics
	ServiceMetrics map[string]*ServiceTraceMetrics
}

// ServiceTraceMetrics holds metrics for a service
type ServiceTraceMetrics struct {
	ServiceName   string
	SpanCount     uint64
	ErrorCount    uint64
	TotalDuration time.Duration
	Durations     []time.Duration
}

// StoredSpan represents a span to be forwarded to the backend
type StoredSpan struct {
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
	ParentSpanID  string            `json:"parent_span_id"`
	ServiceName   string            `json:"service_name"`
	SpanName      string            `json:"span_name"`
	SpanKind      int               `json:"span_kind"`
	Timestamp     int64             `json:"timestamp"`
	DurationNs    uint64            `json:"duration_ns"`
	StatusCode    int               `json:"status_code"`
	StatusMessage string            `json:"status_message"`
	Attributes    map[string]string `json:"attributes"`
	Events        string            `json:"events"`
	HTTPMethod    string            `json:"http_method"`
	HTTPStatusCode int              `json:"http_status_code"`
	HTTPURL       string            `json:"http_url"`
	ClusterID     string            `json:"cluster_id"`
}

// OTelReceiver receives OpenTelemetry traces via HTTP
type OTelReceiver struct {
	server    *http.Server
	clusterID string
	nodeName  string

	metrics    *TraceMetrics
	spanBuffer []StoredSpan
	spanMu     sync.Mutex
	stopCh     chan struct{}
}

// NewOTelReceiver creates a new OpenTelemetry receiver
func NewOTelReceiver(port int, clusterID, nodeName string) *OTelReceiver {
	r := &OTelReceiver{
		clusterID: clusterID,
		nodeName:  nodeName,
		metrics: &TraceMetrics{
			ServiceMetrics: make(map[string]*ServiceTraceMetrics),
		},
		spanBuffer: make([]StoredSpan, 0, 1000),
		stopCh:     make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", r.handleTraces)
	mux.HandleFunc("/health", r.handleHealth)

	r.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return r
}

// CollectSpans returns buffered spans and clears the buffer
func (r *OTelReceiver) CollectSpans() []StoredSpan {
	r.spanMu.Lock()
	defer r.spanMu.Unlock()

	spans := r.spanBuffer
	r.spanBuffer = make([]StoredSpan, 0, 1000)
	return spans
}

// Start begins listening for traces
func (r *OTelReceiver) Start() error {
	go func() {
		fmt.Printf("OpenTelemetry receiver listening on %s\n", r.server.Addr)
		if err := r.server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("OTel receiver error: %v\n", err)
		}
	}()
	return nil
}

// Stop stops the receiver
func (r *OTelReceiver) Stop() {
	close(r.stopCh)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.server.Shutdown(ctx)
}

func (r *OTelReceiver) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (r *OTelReceiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	contentType := req.Header.Get("Content-Type")

	body, err := io.ReadAll(io.LimitReader(req.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		http.Error(w, "Error reading body", http.StatusBadRequest)
		return
	}
	defer func() { _ = req.Body.Close() }()

	// Handle protobuf (default from Python OTel SDK)
	if strings.Contains(contentType, "protobuf") {
		var protoReq collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &protoReq); err != nil {
			fmt.Printf("Error parsing protobuf traces: %v\n", err)
			http.Error(w, "Invalid protobuf", http.StatusBadRequest)
			return
		}

		spanCount := 0
		var newSpans []StoredSpan

		for _, rs := range protoReq.ResourceSpans {
			serviceName := "unknown"
			if rs.Resource != nil {
				for _, attr := range rs.Resource.Attributes {
					if attr.Key == "service.name" {
						serviceName = attr.Value.GetStringValue()
						break
					}
				}
			}

			r.metrics.mu.Lock()
			sm, ok := r.metrics.ServiceMetrics[serviceName]
			if !ok {
				sm = &ServiceTraceMetrics{
					ServiceName: serviceName,
					Durations:   make([]time.Duration, 0, 1000),
				}
				r.metrics.ServiceMetrics[serviceName] = sm
			}

			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					spanCount++
					sm.SpanCount++

					durationNs := uint64(0)
					if span.EndTimeUnixNano > span.StartTimeUnixNano {
						durationNs = span.EndTimeUnixNano - span.StartTimeUnixNano
						duration := time.Duration(durationNs)
						sm.TotalDuration += duration
						sm.Durations = append(sm.Durations, duration)

						if len(sm.Durations) > 10000 {
							sm.Durations = sm.Durations[len(sm.Durations)-5000:]
						}
					}

					statusCode := 0
					statusMessage := ""
					if span.Status != nil {
						statusCode = int(span.Status.Code)
						statusMessage = span.Status.Message
						if statusCode == 2 {
							sm.ErrorCount++
						}
					}

					// Extract attributes
					attrs := make(map[string]string)
					httpMethod := ""
					httpStatusCode := 0
					httpURL := ""
					for _, attr := range span.Attributes {
						key := attr.Key
						val := ""
						if attr.Value != nil {
							if sv := attr.Value.GetStringValue(); sv != "" {
								val = sv
							} else if iv := attr.Value.GetIntValue(); iv != 0 {
								val = fmt.Sprintf("%d", iv)
							} else if bv := attr.Value.GetBoolValue(); bv {
								val = "true"
							}
						}
						attrs[key] = val

						switch key {
						case "http.method":
							httpMethod = val
						case "http.status_code":
							httpStatusCode, _ = parseInt(val)
						case "http.url", "http.target":
							httpURL = val
						}
					}

					// Store span
					storedSpan := StoredSpan{
						TraceID:        fmt.Sprintf("%x", span.TraceId),
						SpanID:         fmt.Sprintf("%x", span.SpanId),
						ParentSpanID:   fmt.Sprintf("%x", span.ParentSpanId),
						ServiceName:    serviceName,
						SpanName:       span.Name,
						SpanKind:       int(span.Kind),
						Timestamp:      int64(span.StartTimeUnixNano / 1e6), // Convert to ms
						DurationNs:     durationNs,
						StatusCode:     statusCode,
						StatusMessage:  statusMessage,
						Attributes:     attrs,
						Events:         "[]",
						HTTPMethod:     httpMethod,
						HTTPStatusCode: httpStatusCode,
						HTTPURL:        httpURL,
						ClusterID:      r.clusterID,
					}
					newSpans = append(newSpans, storedSpan)
				}
			}
			r.metrics.mu.Unlock()
		}

		// Buffer spans
		if len(newSpans) > 0 {
			r.spanMu.Lock()
			r.spanBuffer = append(r.spanBuffer, newSpans...)
			// Keep buffer bounded
			if len(r.spanBuffer) > 10000 {
				r.spanBuffer = r.spanBuffer[len(r.spanBuffer)-5000:]
			}
			r.spanMu.Unlock()
		}

		fmt.Printf("Received %d spans (protobuf) from trace request\n", spanCount)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
		return
	}

	// Handle JSON
	var traceReq OTelTraceRequest
	if err := json.Unmarshal(body, &traceReq); err != nil {
		fmt.Printf("Warning: Could not parse trace as JSON: %v\n", err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"partialSuccess":{}}`))
		return
	}

	spanCount := 0
	for _, rs := range traceReq.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			spanCount += len(ss.Spans)
		}
	}
	fmt.Printf("Received %d spans (JSON) from trace request\n", spanCount)

	r.processTraces(&traceReq)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"partialSuccess":{}}`))
}

func (r *OTelReceiver) processTraces(req *OTelTraceRequest) {
	r.metrics.mu.Lock()
	defer r.metrics.mu.Unlock()

	for _, rs := range req.ResourceSpans {
		serviceName := "unknown"
		if rs.Resource != nil {
			if sn, ok := rs.Resource.Attributes["service.name"]; ok {
				if s, ok := sn.(string); ok {
					serviceName = s
				}
			}
		}

		sm, ok := r.metrics.ServiceMetrics[serviceName]
		if !ok {
			sm = &ServiceTraceMetrics{
				ServiceName: serviceName,
				Durations:   make([]time.Duration, 0, 1000),
			}
			r.metrics.ServiceMetrics[serviceName] = sm
		}

		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				sm.SpanCount++

				// Calculate duration
				if span.EndTimeNs > span.StartTimeNs {
					duration := time.Duration(span.EndTimeNs - span.StartTimeNs)
					sm.TotalDuration += duration
					sm.Durations = append(sm.Durations, duration)

					// Keep bounded
					if len(sm.Durations) > 10000 {
						sm.Durations = sm.Durations[len(sm.Durations)-5000:]
					}
				}

				// Check for errors
				if span.Status != nil && span.Status.Code == 2 { // ERROR status
					sm.ErrorCount++
				}
			}
		}
	}
}

// CollectMetrics returns trace metrics and resets counters
func (r *OTelReceiver) CollectMetrics(timestamp int64) []Metric {
	r.metrics.mu.Lock()
	defer r.metrics.mu.Unlock()

	var metrics []Metric

	for serviceName, sm := range r.metrics.ServiceMetrics {
		if sm.SpanCount == 0 {
			continue
		}

		tags := map[string]string{
			"service": serviceName,
			"node":    r.nodeName,
		}

		// Span count
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "trace.span_count",
			Value:      float64(sm.SpanCount),
			Unit:       "count",
			Tags:       tags,
			ClusterID:  r.clusterID,
			NodeName:   r.nodeName,
		})

		// Error rate
		if sm.SpanCount > 0 {
			errorRate := float64(sm.ErrorCount) / float64(sm.SpanCount) * 100
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "trace.error_rate",
				Value:      errorRate,
				Unit:       "percent",
				Tags:       tags,
				ClusterID:  r.clusterID,
				NodeName:   r.nodeName,
			})
		}

		// Average duration
		if sm.SpanCount > 0 {
			avgDuration := sm.TotalDuration.Milliseconds() / int64(sm.SpanCount)
			metrics = append(metrics, Metric{
				Timestamp:  timestamp,
				MetricName: "trace.duration_avg",
				Value:      float64(avgDuration),
				Unit:       "ms",
				Tags:       tags,
				ClusterID:  r.clusterID,
				NodeName:   r.nodeName,
			})
		}

		// Percentile durations
		if len(sm.Durations) > 0 {
			p50, p95, p99 := calculateTracePercentiles(sm.Durations)
			metrics = append(metrics,
				Metric{
					Timestamp:  timestamp,
					MetricName: "trace.duration_p50",
					Value:      float64(p50.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  r.clusterID,
					NodeName:   r.nodeName,
				},
				Metric{
					Timestamp:  timestamp,
					MetricName: "trace.duration_p95",
					Value:      float64(p95.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  r.clusterID,
					NodeName:   r.nodeName,
				},
				Metric{
					Timestamp:  timestamp,
					MetricName: "trace.duration_p99",
					Value:      float64(p99.Milliseconds()),
					Unit:       "ms",
					Tags:       tags,
					ClusterID:  r.clusterID,
					NodeName:   r.nodeName,
				},
			)
		}

		// Reset for next interval
		delete(r.metrics.ServiceMetrics, serviceName)
	}

	return metrics
}

func calculateTracePercentiles(durations []time.Duration) (p50, p95, p99 time.Duration) {
	n := len(durations)
	if n == 0 {
		return 0, 0, 0
	}

	// Simple sort - could optimize with quickselect
	sorted := make([]time.Duration, n)
	copy(sorted, durations)
	for i := 0; i < n; i++ {
		for j := i + 1; j < n; j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	p50 = sorted[n*50/100]
	p95 = sorted[n*95/100]
	if n*99/100 < n {
		p99 = sorted[n*99/100]
	} else {
		p99 = sorted[n-1]
	}

	return p50, p95, p99
}

func parseInt(s string) (int, error) {
	var result int
	for _, c := range s {
		if c >= '0' && c <= '9' {
			result = result*10 + int(c-'0')
		} else {
			return result, fmt.Errorf("not a number")
		}
	}
	return result, nil
}
