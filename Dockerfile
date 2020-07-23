# Build stage with eBPF support
FROM golang:1.23-bookworm AS builder

WORKDIR /app

# Install dependencies for eBPF compilation with CO:RE support
RUN apt-get update && apt-get install -y \
    git \
    clang \
    llvm \
    libbpf-dev \
    linux-libc-dev \
    gcc-x86-64-linux-gnu \
    && rm -rf /var/lib/apt/lists/*

# Copy go mod files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source
COPY . .

# Create asm symlink for x86_64 kernel headers
RUN ln -sf /usr/include/x86_64-linux-gnu/asm /usr/include/asm

# Compile eBPF program with CO:RE (Compile Once - Run Everywhere)
RUN if [ -f ebpf/http_trace.c ]; then \
    clang -O2 -g \
        -target bpf \
        -D__TARGET_ARCH_x86 \
        -I/usr/include \
        -c ebpf/http_trace.c \
        -o ebpf/http_trace.o \
    && echo "eBPF program compiled successfully" \
    || (echo "eBPF compilation failed" && exit 0); \
    fi

# Build Go binary with CGO for eBPF support
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-w -s" -o cased-agent .

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libbpf1 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/cased-agent /usr/local/bin/cased-agent

# Copy eBPF program if compiled (create empty dir if not)
RUN mkdir -p /ebpf
COPY --from=builder /app/ebpf/http_trace.o* /ebpf/

# Set working directory (eBPF loader looks for ebpf/http_trace.o relative to cwd)
WORKDIR /

# Run as root for cgroup/eBPF access
ENTRYPOINT ["/usr/local/bin/cased-agent"]
