package cmd

import (
	"fmt"
	"os"

	"github.com/google/uuid"
)

func generateNodeID() string {
	podName := os.Getenv("POD_NAME")
	podNamespace := os.Getenv("POD_NAMESPACE")
	podIP := os.Getenv("POD_IP")
	// running in k8s
	if podName != "" && podNamespace != "" {
		return fmt.Sprintf("router~%s~%s~%s", podIP, podName, podNamespace)
	}
	// running in bare metal
	hostname, err := os.Hostname()
	if err != nil {
		return uuid.NewString()
	}
	return hostname
}
