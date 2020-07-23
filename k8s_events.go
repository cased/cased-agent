package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
)

// K8sEvent represents a Kubernetes event for telemetry
type K8sEvent struct {
	Timestamp       time.Time
	Type            string // Normal, Warning
	Reason          string // e.g., Killing, OOMKilling, FailedScheduling
	Message         string
	InvolvedObject  string // e.g., pod/my-pod
	Namespace       string
	Count           int32
	FirstTimestamp  time.Time
	LastTimestamp   time.Time
}

// K8sEventCollector watches Kubernetes events
type K8sEventCollector struct {
	client    *kubernetes.Clientset
	nodeName  string
	clusterID string

	mu     sync.Mutex
	events []K8sEvent
	stopCh chan struct{}
}

// NewK8sEventCollector creates a new Kubernetes event collector
func NewK8sEventCollector(client *kubernetes.Clientset, nodeName, clusterID string) *K8sEventCollector {
	return &K8sEventCollector{
		client:    client,
		nodeName:  nodeName,
		clusterID: clusterID,
		events:    make([]K8sEvent, 0),
		stopCh:    make(chan struct{}),
	}
}

// Start begins watching Kubernetes events
func (k *K8sEventCollector) Start(ctx context.Context) {
	if k.client == nil {
		fmt.Println("K8s client not available, event collection disabled")
		return
	}

	go k.watchEvents(ctx)
}

// Stop stops the event collector
func (k *K8sEventCollector) Stop() {
	close(k.stopCh)
}

func (k *K8sEventCollector) watchEvents(ctx context.Context) {
	for {
		select {
		case <-k.stopCh:
			return
		case <-ctx.Done():
			return
		default:
		}

		// Watch events across all namespaces
		watcher, err := k.client.CoreV1().Events("").Watch(ctx, metav1.ListOptions{
			// Only get events from last 5 minutes to avoid flooding on startup
			ResourceVersion: "",
		})
		if err != nil {
			fmt.Printf("Error watching events: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		k.processEvents(ctx, watcher)
	}
}

func (k *K8sEventCollector) processEvents(ctx context.Context, watcher watch.Interface) {
	defer watcher.Stop()

	for {
		select {
		case <-k.stopCh:
			return
		case <-ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return
			}

			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}

			k8sEvent, ok := event.Object.(*corev1.Event)
			if !ok {
				continue
			}

			// Filter for important events
			if !k.isImportantEvent(k8sEvent) {
				continue
			}

			k.addEvent(k8sEvent)
		}
	}
}

func (k *K8sEventCollector) isImportantEvent(event *corev1.Event) bool {
	// Always include warnings
	if event.Type == "Warning" {
		return true
	}

	// Include specific Normal events that are operationally important
	importantReasons := map[string]bool{
		"Started":           true,
		"Killing":           true,
		"Preempting":        true,
		"Scheduled":         true,
		"SuccessfulCreate":  true,
		"ScalingReplicaSet": true,
		"Pulling":           true,
		"Pulled":            true,
	}

	return importantReasons[event.Reason]
}

func (k *K8sEventCollector) addEvent(event *corev1.Event) {
	k.mu.Lock()
	defer k.mu.Unlock()

	involvedObject := fmt.Sprintf("%s/%s", event.InvolvedObject.Kind, event.InvolvedObject.Name)

	k.events = append(k.events, K8sEvent{
		Timestamp:      time.Now(),
		Type:           event.Type,
		Reason:         event.Reason,
		Message:        event.Message,
		InvolvedObject: involvedObject,
		Namespace:      event.Namespace,
		Count:          event.Count,
		FirstTimestamp: event.FirstTimestamp.Time,
		LastTimestamp:  event.LastTimestamp.Time,
	})

	// Keep bounded
	if len(k.events) > 1000 {
		k.events = k.events[len(k.events)-500:]
	}
}

// CollectMetrics returns events as metrics and clears the buffer
func (k *K8sEventCollector) CollectMetrics(timestamp int64) []Metric {
	k.mu.Lock()
	defer k.mu.Unlock()

	var metrics []Metric

	// Aggregate events by type and reason
	eventCounts := make(map[string]int)
	for _, evt := range k.events {
		key := evt.Type + ":" + evt.Reason
		eventCounts[key]++
	}

	for key, count := range eventCounts {
		// Parse type and reason from key
		var eventType, reason string
		for i, c := range key {
			if c == ':' {
				eventType = key[:i]
				reason = key[i+1:]
				break
			}
		}

		tags := map[string]string{
			"type":   eventType,
			"reason": reason,
			"node":   k.nodeName,
		}

		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.event_count",
			Value:      float64(count),
			Unit:       "count",
			Tags:       tags,
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	// Count warning events specifically
	warningCount := 0
	for _, evt := range k.events {
		if evt.Type == "Warning" {
			warningCount++
		}
	}

	if warningCount > 0 {
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.warning_events",
			Value:      float64(warningCount),
			Unit:       "count",
			Tags:       map[string]string{"node": k.nodeName},
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	// Specific high-signal events
	oomCount := 0
	evictionCount := 0
	failedSchedulingCount := 0
	crashLoopCount := 0

	for _, evt := range k.events {
		switch evt.Reason {
		case "OOMKilling", "OOMKilled":
			oomCount++
		case "Evicted":
			evictionCount++
		case "FailedScheduling":
			failedSchedulingCount++
		case "BackOff":
			crashLoopCount++
		}
	}

	if oomCount > 0 {
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.oom_kills",
			Value:      float64(oomCount),
			Unit:       "count",
			Tags:       map[string]string{"node": k.nodeName},
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	if evictionCount > 0 {
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.evictions",
			Value:      float64(evictionCount),
			Unit:       "count",
			Tags:       map[string]string{"node": k.nodeName},
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	if failedSchedulingCount > 0 {
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.failed_scheduling",
			Value:      float64(failedSchedulingCount),
			Unit:       "count",
			Tags:       map[string]string{"node": k.nodeName},
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	if crashLoopCount > 0 {
		metrics = append(metrics, Metric{
			Timestamp:  timestamp,
			MetricName: "k8s.crashloop_backoff",
			Value:      float64(crashLoopCount),
			Unit:       "count",
			Tags:       map[string]string{"node": k.nodeName},
			ClusterID:  k.clusterID,
			NodeName:   k.nodeName,
		})
	}

	// Clear processed events
	k.events = k.events[:0]

	return metrics
}

// GetRecentEvents returns recent events for querying
func (k *K8sEventCollector) GetRecentEvents() []K8sEvent {
	k.mu.Lock()
	defer k.mu.Unlock()

	events := make([]K8sEvent, len(k.events))
	copy(events, k.events)
	return events
}
