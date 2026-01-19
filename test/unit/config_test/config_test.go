package configtest

import (
	"context"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
)

func TestConfigLoad(t *testing.T) {
	path := "./tmp/config.yaml"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage, err := config.Initialization(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := storage.GetConfig().Backends[0].EndpointSelector.(*config.RoundRobinSelector); !ok {
		t.Fatal("is not the round_robin selector")
	}
	t.Logf("%+v", storage.GetConfig().Backends[0].Capabilty.Endpoints[0])
}
