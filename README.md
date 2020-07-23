# cased-agent

[![CI](https://github.com/cased/cased-agent/actions/workflows/ci.yaml/badge.svg)](https://github.com/cased/cased-agent/actions/workflows/ci.yaml)

Lightweight Kubernetes metrics collector for [Cased](https://cased.com) observability. Runs as a DaemonSet on every node, collecting container metrics, Kubernetes events, and optionally HTTP traffic via eBPF.

## Quick Start

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/cased/cased-agent/main/install.sh | bash -s -- \
  --api-key YOUR_CASED_API_KEY \
  --cluster-id prod
```

### Helm

```bash
helm install cased-agent oci://ghcr.io/cased/charts/cased-agent \
  --namespace cased-system \
  --create-namespace \
  --set apiKey=YOUR_CASED_API_KEY \
  --set clusterId=prod
```

### kubectl

```bash
kubectl apply -f https://raw.githubusercontent.com/cased/cased-agent/main/deploy/manifests/install.yaml
kubectl -n cased-system create secret generic cased-agent --from-literal=api-key=YOUR_API_KEY
kubectl -n cased-system set env daemonset/cased-agent CASED_CLUSTER_ID=prod
```

## What it collects

### Node metrics
- CPU usage (user, system, idle, iowait)
- Memory (total, used, available, cached, swap)
- Network I/O (bytes, packets, errors per interface)

### Container metrics (per pod/container)
- CPU usage percentage
- **CPU throttling** (throttle percent, throttled time)
- Memory usage, limit, percentage
- **Memory breakdown** (RSS, cache, swap)
- Network throughput (rx/tx bytes per second)
- Disk I/O (read/write bytes per second)

### Kubernetes events
- OOM kills
- Pod evictions
- Failed scheduling
- CrashLoopBackOff
- All Warning events

### HTTP metrics (via eBPF - optional)
- Request count
- Error rate (4xx/5xx)
- Latency percentiles (P50, P95, P99)
- Per-path breakdowns

### OpenTelemetry traces (optional)
- Span count by service
- Trace error rate
- Duration percentiles (P50, P95, P99)

### Kubernetes metadata
- Cluster ID
- Node name
- Namespace
- Pod name and UID
- Container name
- Labels

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--endpoint` | `CASED_API_ENDPOINT` | `https://app.cased.com` | Cased API endpoint |
| `--api-key` | `CASED_API_KEY` | - | API key (required) |
| `--cluster-id` | `CASED_CLUSTER_ID` | - | Cluster identifier (required) |
| `--node-name` | `NODE_NAME` | - | Node name |
| `--interval` | - | `15s` | Collection interval |
| `--batch-size` | - | `100` | Max metrics per batch |
| `--enable-ebpf` | `ENABLE_EBPF` | `false` | Enable eBPF HTTP tracing |
| `--enable-otel` | `ENABLE_OTEL` | `false` | Enable OpenTelemetry receiver |
| `--otel-port` | - | `4318` | Port for OTLP HTTP receiver |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Node                             │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                        cased-agent                            │  │
│  │                                                               │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐          │  │
│  │  │  /proc/*     │ │ /sys/fs/     │ │  Kubernetes  │          │  │
│  │  │  (node)      │ │ cgroup/*     │ │     API      │          │  │
│  │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘          │  │
│  │         │                │                │                   │  │
│  │  ┌──────┴────────────────┴────────────────┴──────┐           │  │
│  │  │               Core Collector                   │           │  │
│  │  │  CPU, Memory, Network, Disk, K8s Events       │           │  │
│  │  └────────────────────┬───────────────────────────┘           │  │
│  │                       │                                       │  │
│  │  ┌────────────────────┼────────────────────┐                 │  │
│  │  │                    │                    │                  │  │
│  │  ▼                    ▼                    ▼                  │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐          │  │
│  │  │    eBPF      │ │    OTel      │ │   Batch &    │          │  │
│  │  │  HTTP Trace  │ │  Receiver    │ │    Send      │          │  │
│  │  │  (optional)  │ │  :4318       │ │              │          │  │
│  │  └──────────────┘ └──────────────┘ └──────┬───────┘          │  │
│  │                                           │                   │  │
│  └───────────────────────────────────────────┼───────────────────┘  │
│                                              │                      │
└──────────────────────────────────────────────┼──────────────────────┘
                                               │ HTTPS POST
                                               ▼
                                     ┌─────────────────┐
                                     │    Cased API    │
                                     │   /api/v1/      │
                                     │   telemetry/    │
                                     │   metrics       │
                                     └─────────────────┘
```

## OpenTelemetry Integration

To send traces to the agent's OTel receiver, configure your application's OTLP exporter:

```bash
# Environment variables for OTLP
export OTEL_EXPORTER_OTLP_ENDPOINT=http://cased-agent:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http/json
```

Or in code:
```python
# Python example
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

exporter = OTLPSpanExporter(endpoint="http://cased-agent:4318/v1/traces")
```

## Local Development

### Using Docker Compose

```bash
# Start with sample workloads
docker compose up --build

# View agent logs
docker compose logs agent -f
```

The compose file includes:
- **agent**: The metrics collector
- **workload**: CPU/memory stress test
- **web**: Sample HTTP server with random errors
- **traffic**: Generates HTTP traffic to the web service

### Build Manually

```bash
# Build (without eBPF)
CGO_ENABLED=0 go build -o cased-agent .

# Build with eBPF support (Linux only)
clang -O2 -g -target bpf -c ebpf/http_trace.c -o ebpf/http_trace.o
CGO_ENABLED=1 go build -o cased-agent .
```

## Metrics Reference

### Container Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `container.cpu.usage_percent` | percent | CPU utilization |
| `container.cpu.throttle_percent` | percent | CPU throttling rate |
| `container.cpu.throttled_time` | ms/sec | Time spent throttled |
| `container.memory.usage` | bytes | Current memory usage |
| `container.memory.limit` | bytes | Memory limit |
| `container.memory.usage_percent` | percent | Memory utilization |
| `container.memory.rss` | bytes | Resident Set Size |
| `container.memory.cache` | bytes | Page cache |
| `container.memory.swap` | bytes | Swap usage |
| `container.disk.read_bytes_per_sec` | bytes/sec | Disk read throughput |
| `container.disk.write_bytes_per_sec` | bytes/sec | Disk write throughput |

### HTTP Metrics (eBPF)
| Metric | Unit | Description |
|--------|------|-------------|
| `http.request_count` | count | Request count |
| `http.error_rate` | percent | 4xx/5xx rate |
| `http.latency_avg` | ms | Average latency |
| `http.latency_p50` | ms | P50 latency |
| `http.latency_p95` | ms | P95 latency |
| `http.latency_p99` | ms | P99 latency |

### Trace Metrics (OTel)
| Metric | Unit | Description |
|--------|------|-------------|
| `trace.span_count` | count | Spans received |
| `trace.error_rate` | percent | Error span rate |
| `trace.duration_avg` | ms | Average span duration |
| `trace.duration_p50` | ms | P50 duration |
| `trace.duration_p95` | ms | P95 duration |
| `trace.duration_p99` | ms | P99 duration |

### K8s Event Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `k8s.event_count` | count | Events by type/reason |
| `k8s.warning_events` | count | Warning events |
| `k8s.oom_kills` | count | OOM kill events |
| `k8s.evictions` | count | Pod evictions |
| `k8s.failed_scheduling` | count | Scheduling failures |
| `k8s.crashloop_backoff` | count | CrashLoopBackOff events |

## Uninstall

```bash
kubectl delete -f https://raw.githubusercontent.com/cased/cased-agent/main/deploy/manifests/install.yaml
```

Or with Helm:
```bash
helm uninstall cased-agent -n cased-system
```
