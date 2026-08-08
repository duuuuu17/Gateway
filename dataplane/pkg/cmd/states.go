package cmd

type ConfigState int

const (
	StateRemoteHealthy ConfigState = iota
	StateRemoteUnhealthy
	StateFileFallback
	StateMinimal
)
