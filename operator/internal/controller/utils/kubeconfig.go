package utils

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
)

// 使用controller-runtime的加载KubeClient
func GetKubeClient() (*kubernetes.Clientset, error) {
	kubeConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}
	return kubeClient, nil
}

// 手动指定加载kubeconfig文件
func LoadKubeConfigFromLocalForDevUsing(path string) (kubernetes.Interface, error) {
	kubeconfig, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		kubeconfig, err = GenerateKubeconfig()
	}
	return kubernetes.NewForConfig(kubeconfig)
}

// 自动生成kubeconfig
func GenerateKubeconfig() (*rest.Config, error) {
	kubeConfig, err := rest.InClusterConfig()
	if err == nil {
		return kubeConfig, nil
	}
	kubeConfigRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeConfigOverride := clientcmd.ConfigOverrides{}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(kubeConfigRules, &kubeConfigOverride)
	return clientConfig.ClientConfig()
}
