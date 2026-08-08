#!/bin/bash
set -euo pipefail

# 默认值（根据你的实际情况修改）
CONTROL_NAMESPACE="${CONTROL_NAMESPACE:-operator-system}"
DATA_NAMESPACE="${DATA_NAMESPACE:-default}"
CONTROL_RELEASE_NAME="${CONTROL_RELEASE_NAME:-control-plane}"
DATA_RELEASE_NAME="${DATA_RELEASE_NAME:-dataplane}"
CONTROL_SERVICE_NAME="${CONTROL_SERVICE_NAME:-operator-controller-manager}"  # 固定 Service 名称
JAEGER_ENDPOINT="${JAEGER_ENDPOINT:-jaeger-collector.istio-system.svc.cluster.local:4317}"

# 可接收命令行参数覆盖
while [[ $# -gt 0 ]]; do
  case $1 in
    --control-ns) CONTROL_NAMESPACE="$2"; shift 2 ;;
    --data-ns) DATA_NAMESPACE="$2"; shift 2 ;;
    --control-release) CONTROL_RELEASE_NAME="$2"; shift 2 ;;
    --data-release) DATA_RELEASE_NAME="$2"; shift 2 ;;
    --control-svc) CONTROL_SERVICE_NAME="$2"; shift 2 ;;
    --jaeger) JAEGER_ENDPOINT="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "Deploying control-plane with release: $CONTROL_RELEASE_NAME in namespace: $CONTROL_NAMESPACE"
helm upgrade --install "$CONTROL_RELEASE_NAME" ./control-plane \
  --namespace "$CONTROL_NAMESPACE" \
  --create-namespace \
  --wait

# 构建 control-plane Service 的 FQDN
CONTROL_ENDPOINT="${CONTROL_SERVICE_NAME}.${CONTROL_NAMESPACE}.svc.cluster.local:50051"

echo "Deploying dataplane with release: $DATA_RELEASE_NAME in namespace: $DATA_NAMESPACE"
helm upgrade --install "$DATA_RELEASE_NAME" ./dataplane \
  --namespace "$DATA_NAMESPACE" \
  --create-namespace \
  --set controlPlane.endpoint="$CONTROL_ENDPOINT" \
  --set otel.endpoint="$JAEGER_ENDPOINT" \
  --wait

echo "Deployment completed successfully."
echo "Control Plane endpoint: $CONTROL_ENDPOINT"
echo "Jaeger endpoint: $JAEGER_ENDPOINT"

# 默认执行一键部署
# ./deploy.sh --data-ns default --data-release dataplane --control-ns default --control-release control-plane

# 分开部署
# helm install dataplane . -f values.yaml
# helm install control-plane ./chart -f ./chart/values.yaml 