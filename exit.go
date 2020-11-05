package main

import (
	"github.com/spf13/viper"
	"gopkg.in/cyverse-de/messaging.v8"
)

func cleanup(cfg *viper.Viper) {
}

// Exit handles clean up when road-runner is killed.
func Exit(cfg *viper.Viper, exit, finalExit chan messaging.StatusCode) {
	exitCode := <-exit
	log.Warnf("Received an exit code of %d, cleaning up", int(exitCode))
	cleanup(cfg)
	finalExit <- exitCode
}
