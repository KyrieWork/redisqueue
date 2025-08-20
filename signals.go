package redisqueue

import (
	"os"
	"os/signal"
	"syscall"
)

// newSignalHandle creates a channel that receives OS signals
// It handles graceful shutdown on SIGINT/SIGTERM, and force exit on second signal
func newSignalHandle() <-chan struct{} {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // Force exit on second signal
	}()
	return stop
}
