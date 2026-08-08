#!/bin/bash
IMAGE_NAME="${1:-llm-router-dataplane}"
cd ./operator && make deploy IMG=${IMAGE_NAME}
cd ../k8s/kustomize
docker build -t ${IMAGE_NAME}:latest ../../
docker tag ${IMAGE_NAME}:latest  crpi-jok0hxci4hqtytw8.cn-shenzhen.personal.cr.aliyuncs.com/djy-repository/${IMAGE_NAME}:latest
docker push crpi-jok0hxci4hqtytw8.cn-shenzhen.personal.cr.aliyuncs.com/djy-repository/${IMAGE_NAME}:latest 
kubectl delete -f deployment.yaml  -f llm-router-pod-monitor.yml 2>/dev/null
kubectl apply -f deployment.yaml  -f llm-router-pod-monitor.yml 

kubectl port-forward svc/tracing -n istio-system 30800:80 &
kubectl port-forward svc/prometheus-grafana -n monitoring 3000:80 &