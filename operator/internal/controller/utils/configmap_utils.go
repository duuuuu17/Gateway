package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/v1alpha1"
	"go.yaml.in/yaml/v3"
)

func GenerateConfigMapName(cr configv1alpha1.LLMRouterConfig) string {
	return fmt.Sprintf("llm-router-config-%s-%s", cr.Name, cr.Namespace)
}
func BuildConfigMapData(cr configv1alpha1.LLMRouterConfig) (map[string]string, error) {
	yamlBytes, err := yaml.Marshal(&cr.Spec.Backends)
	if err != nil {
		return nil, err
	}
	return map[string]string{"config.yaml": string(yamlBytes)}, nil
}
func ComputeMapHash(data map[string]string) string {
	b, _ := json.Marshal(data)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
