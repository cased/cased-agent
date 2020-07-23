//go:build !linux
// +build !linux

package main

// EBPFCollector stub for non-Linux systems
type EBPFCollector struct {
	enabled bool
}

// NewEBPFCollector returns a disabled collector on non-Linux
func NewEBPFCollector() (*EBPFCollector, error) {
	return &EBPFCollector{enabled: false}, nil
}

// Start is a no-op on non-Linux
func (e *EBPFCollector) Start() {}

// Stop is a no-op on non-Linux
func (e *EBPFCollector) Stop() {}

// CollectMetrics returns nil on non-Linux
func (e *EBPFCollector) CollectMetrics(timestamp int64, clusterID, nodeName string) []Metric {
	return nil
}

// IsEnabled returns false on non-Linux
func (e *EBPFCollector) IsEnabled() bool {
	return false
}
