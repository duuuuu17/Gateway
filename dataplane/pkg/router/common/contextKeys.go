package common

import (
	"context"
)

type contextStrKey string

var (
	keyModelName contextStrKey = "x-llm-model"
)

func SetModelName(ctx context.Context, modelName string) context.Context {
	return context.WithValue(ctx, keyModelName, modelName)
}
func GetModelNameRetStr(ctx context.Context) string {
	if v, ok := ctx.Value(keyModelName).(string); ok {
		return v
	}
	return ""
}
