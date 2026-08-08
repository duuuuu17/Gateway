package llmrouterxds

import (
	"encoding/json"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"
	runtime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// string format: 'type/resourceName'
func BuildXDSKey(xdsType XDSType, resourceName string) string {
	return string(xdsType) + "/" + resourceName
}
func ParseXDSKey(key string) (XDSType, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return XDSType(parts[0]), parts[1]
}
func setToKeysSlice(m map[string]struct{}) []string {
	res := make([]string, 0, len(m))
	for key, _ := range m {
		res = append(res, key)
	}
	return res
}

// protobuf just support json format that can converted to structpb type
func RawExtensionToStruct(raw runtime.RawExtension) (*structpb.Struct, error) {
	if len(raw.Raw) == 0 {
		return nil, nil
	}
	jsonBytes, err := yaml.YAMLToJSON(raw.Raw)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return nil, err
	}
	// if :
	// 	config:
	//   - key: a
	//   - key: b
	// need using structpb.NewList()
	return structpb.NewStruct(m)
}
