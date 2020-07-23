// eBPF program for HTTP request tracing
// Attaches to socket read/write to capture HTTP traffic
// Uses CO:RE (Compile Once - Run Everywhere) for kernel portability

#include <linux/types.h>
#include <linux/bpf.h>
#include <linux/ptrace.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

// size_t not available in BPF context
typedef unsigned long size_t;

#define MAX_MSG_SIZE 256
#define HTTP_METHOD_LEN 8

// HTTP request event sent to userspace
struct http_event {
    __u64 timestamp_ns;
    __u64 duration_ns;
    __u32 pid;
    __u32 tid;
    __u32 status_code;
    __u32 request_size;
    __u32 response_size;
    __u8 method[HTTP_METHOD_LEN];
    __u8 path[64];
    __u8 container_id[64];
};

// Map to store in-flight requests
struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 10000);
    __type(key, __u64);  // socket fd + tid
    __type(value, struct http_event);
} inflight_requests SEC(".maps");

// Ring buffer for completed HTTP events
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 256 * 1024);
} http_events SEC(".maps");

// Parse HTTP method from buffer
static __always_inline int parse_http_method(const char *buf, int len, struct http_event *evt) {
    if (len < 4) return 0;

    // Check common HTTP methods
    if (buf[0] == 'G' && buf[1] == 'E' && buf[2] == 'T' && buf[3] == ' ') {
        __builtin_memcpy(evt->method, "GET", 4);
        return 4;
    }
    if (len >= 5 && buf[0] == 'P' && buf[1] == 'O' && buf[2] == 'S' && buf[3] == 'T' && buf[4] == ' ') {
        __builtin_memcpy(evt->method, "POST", 5);
        return 5;
    }
    if (len >= 4 && buf[0] == 'P' && buf[1] == 'U' && buf[2] == 'T' && buf[3] == ' ') {
        __builtin_memcpy(evt->method, "PUT", 4);
        return 4;
    }
    if (len >= 7 && buf[0] == 'D' && buf[1] == 'E' && buf[2] == 'L' && buf[3] == 'E' && buf[4] == 'T' && buf[5] == 'E' && buf[6] == ' ') {
        __builtin_memcpy(evt->method, "DELETE", 7);
        return 7;
    }
    if (len >= 6 && buf[0] == 'P' && buf[1] == 'A' && buf[2] == 'T' && buf[3] == 'C' && buf[4] == 'H' && buf[5] == ' ') {
        __builtin_memcpy(evt->method, "PATCH", 6);
        return 6;
    }

    return 0;
}

// Parse HTTP status code from response
static __always_inline int parse_http_status(const char *buf, int len) {
    // HTTP/1.1 200 OK
    if (len < 12) return 0;
    if (buf[0] != 'H' || buf[1] != 'T' || buf[2] != 'T' || buf[3] != 'P') return 0;

    // Find status code after "HTTP/X.X "
    int i = 8;
    if (i >= len || buf[i] != ' ') return 0;
    i++;

    // Parse 3-digit status code
    if (i + 3 > len) return 0;
    int status = (buf[i] - '0') * 100 + (buf[i+1] - '0') * 10 + (buf[i+2] - '0');

    if (status >= 100 && status < 600) return status;
    return 0;
}

// Kprobe on socket write (request sent)
SEC("kprobe/__sys_sendto")
int trace_send(struct pt_regs *ctx) {
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 pid = pid_tgid >> 32;
    __u32 tid = pid_tgid;

    int fd = PT_REGS_PARM1(ctx);
    char *buf = (char *)PT_REGS_PARM2(ctx);
    size_t len = PT_REGS_PARM3(ctx);

    if (len < 4 || len > MAX_MSG_SIZE) return 0;

    char data[MAX_MSG_SIZE];
    if (bpf_probe_read_user(data, sizeof(data), buf) < 0) return 0;

    struct http_event evt = {};
    int method_end = parse_http_method(data, len, &evt);
    if (method_end == 0) return 0;

    // Parse path
    int path_start = method_end;
    int path_end = path_start;
    #pragma unroll
    for (int i = 0; i < 63 && path_start + i < len; i++) {
        if (data[path_start + i] == ' ' || data[path_start + i] == '?' || data[path_start + i] == '\r') {
            path_end = path_start + i;
            break;
        }
        path_end = path_start + i + 1;
    }

    int path_len = path_end - path_start;
    if (path_len > 0 && path_len < 64) {
        // Use bpf_probe_read for bounded copy
        bpf_probe_read_kernel(evt.path, path_len & 63, data + path_start);
    }

    evt.timestamp_ns = bpf_ktime_get_ns();
    evt.pid = pid;
    evt.tid = tid;
    evt.request_size = len;

    // Store in inflight map
    __u64 key = ((__u64)fd << 32) | tid;
    bpf_map_update_elem(&inflight_requests, &key, &evt, BPF_ANY);

    return 0;
}

// Kprobe on socket read (response received)
SEC("kprobe/__sys_recvfrom")
int trace_recv_entry(struct pt_regs *ctx) {
    return 0;
}

SEC("kretprobe/__sys_recvfrom")
int trace_recv_exit(struct pt_regs *ctx) {
    __u64 pid_tgid = bpf_get_current_pid_tgid();
    __u32 tid = pid_tgid;

    long ret = PT_REGS_RC(ctx);
    if (ret <= 0) return 0;

    // Try to find matching request
    // Note: simplified - in production we'd track fd properly
    __u64 key = 0;
    struct http_event *evt;

    // Look for any inflight request from this thread
    #pragma unroll
    for (int i = 0; i < 10; i++) {
        key = ((__u64)i << 32) | tid;
        evt = bpf_map_lookup_elem(&inflight_requests, &key);
        if (evt) break;
    }

    if (!evt) return 0;

    // Calculate duration
    __u64 now = bpf_ktime_get_ns();
    evt->duration_ns = now - evt->timestamp_ns;
    evt->response_size = ret;

    // Send to userspace
    struct http_event *ring_evt = bpf_ringbuf_reserve(&http_events, sizeof(*ring_evt), 0);
    if (ring_evt) {
        __builtin_memcpy(ring_evt, evt, sizeof(*ring_evt));
        bpf_ringbuf_submit(ring_evt, 0);
    }

    bpf_map_delete_elem(&inflight_requests, &key);
    return 0;
}

char LICENSE[] SEC("license") = "GPL";
