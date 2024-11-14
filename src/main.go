package main

import (
	"flag"
	"fmt"
	"time"
)

const VERSION = "0.7.0"

func main() {
	flag.Parse()
	config := LoadConfig()

	if len(flag.Args()) == 0 {
		start(config)
		return
	}

	command := flag.Arg(0)

	switch command {
	case "start":
		start(config)
	case "sync":
		if config.SyncInterval != "" {
			duration, err := time.ParseDuration(config.SyncInterval)
			if err != nil {
				panic("Invalid interval format: " + config.SyncInterval)
			}
			LogInfo(config, "Starting sync loop with interval:", config.SyncInterval)
			for {
				syncFromPg(config)
				LogInfo(config, "Sleeping for", config.SyncInterval)
				time.Sleep(duration)
			}
		} else {
			syncFromPg(config)
		}
	case "version":
		fmt.Println("BemiDB version:", VERSION)
	default:
		panic("Unknown command: " + command)
	}
}

func start(config *Config) {
	tcpListener := NewTcpListener(config)
	LogInfo(config, "BemiDB: Listening on", tcpListener.Addr())

	duckdb := NewDuckdb(config)
	LogInfo(config, "DuckDB: Connected")
	defer duckdb.Close()

	icebergReader := NewIcebergReader(config)

	for {
		conn := AcceptConnection(tcpListener)
		LogInfo(config, "BemiDB: Accepted connection from", conn.RemoteAddr())

		postgres := NewPostgres(config, &conn)
		proxy := NewProxy(config, duckdb, icebergReader)

		go func() {
			postgres.Run(proxy)
			defer postgres.Close()
			LogInfo(config, "BemiDB: Closed connection from", conn.RemoteAddr())
		}()
	}
}

func syncFromPg(config *Config) {
	syncer := NewSyncer(config)
	syncer.SyncFromPostgres()
	LogInfo(config, "Sync from PostgreSQL completed successfully.")
}
