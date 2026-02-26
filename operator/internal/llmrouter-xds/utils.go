package llmrouterxds

import "strings"

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
