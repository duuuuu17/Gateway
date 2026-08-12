#!/bin/bash
helm upgrade --install alloy k8s/observability/slog/alloy/alloy-1.11.0.tgz --create-namespae -n monitoring -f k8s/observability/slog/alloy/alloy-values.yml
helm upgrade --install loki k8s/observability/slog/loki/loki-7.2.0.tgz --create-namespae -n monitoring -f k8s/observability/slog/loki/loki-values.yml
helm upgrade --install prometheus k8s/observability/prometheus/kube-prometheus-stack-88.1.2.tgz --create-namespae -n monitoring -f k8s/observability/prometheus/prom-values.yaml
