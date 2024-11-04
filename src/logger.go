package main

import (
	"log"
)

type LogLevel string

const (
	LOG_LEVEL_DEBUG = "DEBUG"
	LOG_LEVEL_INFO  = "INFO"
	LOG_LEVEL_ERROR = "ERROR"
)

var LOG_LEVELS = []string{
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_INFO,
	LOG_LEVEL_ERROR,
}

func LogError(config *Config, message ...interface{}) {
	log.Println(append([]interface{}{"[ERROR]"}, message...)...)
}

func LogInfo(config *Config, message ...interface{}) {
	if config.LogLevel == LOG_LEVEL_INFO || config.LogLevel == LOG_LEVEL_DEBUG {
		log.Println(append([]interface{}{"[INFO]"}, message...)...)
	}
}

func LogDebug(config *Config, message ...interface{}) {
	if config.LogLevel == LOG_LEVEL_DEBUG {
		log.Println(append([]interface{}{"[DEBUG]"}, message...)...)
	}
}
