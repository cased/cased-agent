#!/bin/bash
#
# Cased Agent Installer
# Usage: curl -fsSL https://raw.githubusercontent.com/cased/cased-agent/main/install.sh | bash -s -- --api-key YOUR_KEY --cluster-id prod
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
NAMESPACE="cased-system"
API_ENDPOINT="https://app.cased.com"
ENABLE_EBPF="true"
MANIFEST_URL="https://raw.githubusercontent.com/cased/cased-agent/main/deploy/manifests/install.yaml"

# Parse arguments
API_KEY=""
CLUSTER_ID=""
UNINSTALL=false
SKIP_CONFIRM=false

print_banner() {
    echo -e "${BLUE}"
    echo "  ██████╗ █████╗ ███████╗███████╗██████╗ "
    echo " ██╔════╝██╔══██╗██╔════╝██╔════╝██╔══██╗"
    echo " ██║     ███████║███████╗█████╗  ██║  ██║"
    echo " ██║     ██╔══██║╚════██║██╔══╝  ██║  ██║"
    echo " ╚██████╗██║  ██║███████║███████╗██████╔╝"
    echo "  ╚═════╝╚═╝  ╚═╝╚══════╝╚══════╝╚═════╝ "
    echo -e "${NC}"
    echo -e "${GREEN}Kubernetes Metrics Agent${NC}"
    echo ""
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --api-key KEY        Cased API key (required)"
    echo "  --cluster-id ID      Cluster identifier (required)"
    echo "  --namespace NS       Kubernetes namespace (default: cased-system)"
    echo "  --endpoint URL       Cased API endpoint (default: https://app.cased.com)"
    echo "  --disable-ebpf       Disable eBPF HTTP tracing"
    echo "  --yes, -y            Skip confirmation prompt"
    echo "  --uninstall          Remove cased-agent from cluster"
    echo "  --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  Install:"
    echo "    $0 --api-key abc123 --cluster-id prod"
    echo ""
    echo "  Uninstall:"
    echo "    $0 --uninstall"
    echo ""
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        log_error "kubectl not found. Please install kubectl first."
        exit 1
    fi
    log_success "kubectl found"

    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        log_error "Cannot connect to Kubernetes cluster. Please check your kubeconfig."
        exit 1
    fi
    log_success "Connected to Kubernetes cluster"
}

confirm_installation() {
    local context=$(kubectl config current-context 2>/dev/null || echo "unknown")
    local server=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}' 2>/dev/null || echo "unknown")
    local nodes=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')

    echo ""
    echo -e "${YELLOW}══════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}                    INSTALLATION SUMMARY                       ${NC}"
    echo -e "${YELLOW}══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  Kubernetes Context:  ${GREEN}$context${NC}"
    echo -e "  API Server:          $server"
    echo -e "  Nodes in cluster:    $nodes"
    echo ""
    echo -e "  Cased Cluster ID:    ${GREEN}$CLUSTER_ID${NC}"
    echo -e "  API Endpoint:        $API_ENDPOINT"
    echo -e "  eBPF HTTP Tracing:   $ENABLE_EBPF"
    echo ""
    echo -e "${YELLOW}══════════════════════════════════════════════════════════════${NC}"
    echo ""

    if [ "$SKIP_CONFIRM" = true ]; then
        log_info "Skipping confirmation (--yes flag)"
        return 0
    fi

    echo -e "${RED}This will install cased-agent as a DaemonSet on ALL $nodes nodes.${NC}"
    echo ""
    read -p "Do you want to proceed? [y/N] " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Installation cancelled."
        exit 0
    fi
    echo ""
}

install_agent() {
    log_info "Installing cased-agent..."

    # Check for existing installation and clean up if needed
    if kubectl get daemonset cased-agent -n "$NAMESPACE" &> /dev/null; then
        log_warn "Existing cased-agent found. Removing before upgrade..."
        kubectl delete daemonset cased-agent -n "$NAMESPACE" --ignore-not-found=true
        sleep 2
    fi

    # Apply manifests
    log_info "Applying manifests from $MANIFEST_URL"
    if ! kubectl apply -f "$MANIFEST_URL"; then
        log_error "Failed to apply manifests"
        exit 1
    fi
    log_success "Manifests applied"

    # Wait for namespace to be ready
    sleep 2

    # Create or update secret
    log_info "Configuring API key..."
    kubectl -n "$NAMESPACE" delete secret cased-agent 2>/dev/null || true
    if ! kubectl -n "$NAMESPACE" create secret generic cased-agent --from-literal=api-key="$API_KEY"; then
        log_error "Failed to create secret"
        exit 1
    fi
    log_success "API key configured"

    # Set cluster ID
    log_info "Setting cluster ID to: $CLUSTER_ID"
    if ! kubectl -n "$NAMESPACE" set env daemonset/cased-agent CASED_CLUSTER_ID="$CLUSTER_ID"; then
        log_error "Failed to set cluster ID"
        exit 1
    fi
    log_success "Cluster ID configured"

    # Set API endpoint if custom
    if [ "$API_ENDPOINT" != "https://app.cased.com" ]; then
        log_info "Setting custom API endpoint: $API_ENDPOINT"
        kubectl -n "$NAMESPACE" set env daemonset/cased-agent CASED_API_ENDPOINT="$API_ENDPOINT"
    fi

    # Configure eBPF
    if [ "$ENABLE_EBPF" = "false" ]; then
        log_info "Disabling eBPF HTTP tracing"
        kubectl -n "$NAMESPACE" set env daemonset/cased-agent ENABLE_EBPF="false"
    fi

    # Wait for rollout
    log_info "Waiting for agent to be ready..."
    if kubectl -n "$NAMESPACE" rollout status daemonset/cased-agent --timeout=120s; then
        log_success "cased-agent is running"
    else
        log_warn "Rollout is taking longer than expected. Check status with:"
        echo "  kubectl -n $NAMESPACE get pods -l app.kubernetes.io/name=cased-agent"
    fi
}

uninstall_agent() {
    log_info "Uninstalling cased-agent..."

    if kubectl get namespace "$NAMESPACE" &> /dev/null; then
        kubectl delete -f "$MANIFEST_URL" 2>/dev/null || true
        log_success "cased-agent uninstalled"
    else
        log_warn "Namespace $NAMESPACE not found. Agent may not be installed."
    fi
}

verify_installation() {
    echo ""
    log_info "Verifying installation..."

    local pods=$(kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=cased-agent -o jsonpath='{.items[*].status.phase}' 2>/dev/null)
    local running=$(echo "$pods" | tr ' ' '\n' | grep -c "Running" || echo "0")
    local total=$(kubectl get nodes --no-headers 2>/dev/null | wc -l | tr -d ' ')

    echo ""
    echo -e "${GREEN}Installation complete!${NC}"
    echo ""
    echo "Status: $running/$total agents running"
    echo ""
    echo "Useful commands:"
    echo "  View agent logs:    kubectl -n $NAMESPACE logs -l app.kubernetes.io/name=cased-agent -f"
    echo "  Check agent status: kubectl -n $NAMESPACE get pods -l app.kubernetes.io/name=cased-agent"
    echo "  View DaemonSet:     kubectl -n $NAMESPACE describe daemonset cased-agent"
    echo ""
    echo "Metrics will appear in your Cased dashboard within a few minutes."
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --api-key)
            API_KEY="$2"
            shift 2
            ;;
        --cluster-id)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --endpoint)
            API_ENDPOINT="$2"
            shift 2
            ;;
        --disable-ebpf)
            ENABLE_EBPF="false"
            shift
            ;;
        --yes|-y)
            SKIP_CONFIRM=true
            shift
            ;;
        --uninstall)
            UNINSTALL=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Main
print_banner
check_prerequisites

if [ "$UNINSTALL" = true ]; then
    uninstall_agent
    exit 0
fi

# Validate required arguments
if [ -z "$API_KEY" ]; then
    log_error "API key is required. Use --api-key YOUR_KEY"
    echo ""
    usage
    exit 1
fi

if [ -z "$CLUSTER_ID" ]; then
    log_error "Cluster ID is required. Use --cluster-id YOUR_CLUSTER"
    echo ""
    usage
    exit 1
fi

confirm_installation
install_agent
verify_installation
