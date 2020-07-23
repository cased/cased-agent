#!/bin/bash
# Build and push cased-agent multi-architecture images
#
# Usage:
#   ./build.sh                    # Build for local platform
#   ./build.sh --push             # Build multi-arch and push to ECR
#   ./build.sh --push --tag v1.0  # Build and push with specific tag
#
# Environment variables:
#   REGISTRY    - Container registry (default: 495860673956.dkr.ecr.us-west-2.amazonaws.com)
#   IMAGE_NAME  - Image name (default: cased-agent)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
REGISTRY="${REGISTRY:-495860673956.dkr.ecr.us-west-2.amazonaws.com}"
IMAGE_NAME="${IMAGE_NAME:-cased-agent}"
TAG="latest"
PUSH=false
PLATFORMS="linux/amd64,linux/arm64"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --push)
            PUSH=true
            shift
            ;;
        --tag)
            TAG="$2"
            shift 2
            ;;
        --registry)
            REGISTRY="$2"
            shift 2
            ;;
        --platform)
            PLATFORMS="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--push] [--tag TAG] [--registry REGISTRY] [--platform PLATFORMS]"
            exit 1
            ;;
    esac
done

FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"

echo "Building cased-agent..."
echo "  Registry: $REGISTRY"
echo "  Image: $IMAGE_NAME:$TAG"
echo "  Platforms: $PLATFORMS"
echo "  Push: $PUSH"
echo ""

# Ensure buildx is available
if ! docker buildx version &>/dev/null; then
    echo "Error: docker buildx is required for multi-arch builds"
    echo "Install with: docker buildx install"
    exit 1
fi

# Create/use buildx builder for multi-platform
BUILDER_NAME="cased-agent-builder"
if ! docker buildx inspect "$BUILDER_NAME" &>/dev/null; then
    echo "Creating buildx builder: $BUILDER_NAME"
    docker buildx create --name "$BUILDER_NAME" --use --bootstrap
else
    docker buildx use "$BUILDER_NAME"
fi

if [ "$PUSH" = true ]; then
    # Login to ECR if pushing to AWS
    if [[ "$REGISTRY" == *".ecr."* ]]; then
        echo "Logging in to ECR..."
        REGION=$(echo "$REGISTRY" | sed 's/.*\.ecr\.\([^.]*\)\..*/\1/')
        aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY"
    fi

    echo ""
    echo "Building and pushing multi-arch image..."
    docker buildx build \
        --platform "$PLATFORMS" \
        --tag "$FULL_IMAGE" \
        --push \
        .

    echo ""
    echo "Successfully pushed: $FULL_IMAGE"
    echo ""
    echo "To deploy to Kubernetes:"
    echo "  kubectl -n cased-system rollout restart ds cased-agent"
else
    # Build for local platform only
    echo "Building for local platform..."
    docker build -t "${IMAGE_NAME}:${TAG}" .

    echo ""
    echo "Successfully built: ${IMAGE_NAME}:${TAG}"
    echo ""
    echo "To push to a registry, run:"
    echo "  $0 --push"
fi
