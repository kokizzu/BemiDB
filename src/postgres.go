package main

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

type Postgres struct {
	backend *pgproto3.Backend
	conn    *net.Conn
	config  *Config
}

func NewPostgres(config *Config, conn *net.Conn) *Postgres {
	return &Postgres{
		conn:    conn,
		backend: pgproto3.NewBackend(*conn, *conn),
		config:  config,
	}
}

func NewTcpListener(config *Config) net.Listener {
	tcpListener, err := net.Listen("tcp", "127.0.0.1:"+config.Port)
	PanicIfError(err)
	return tcpListener
}

func AcceptConnection(listener net.Listener) net.Conn {
	conn, err := listener.Accept()
	PanicIfError(err)
	return conn
}

func (postgres *Postgres) Run(proxy *Proxy) {
	postgres.handleStartup()

	for {
		msg, err := postgres.backend.Receive()
		if err != nil {
			return
		}

		switch msg.(type) {
		case *pgproto3.Query:
			query := msg.(*pgproto3.Query).String
			messages, err := proxy.HandleQuery(query)
			if err != nil {
				postgres.writeError("Internal error")
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Terminate:
			return
		default:
			PanicIfError(fmt.Errorf("Received message other than Query from client: %#v", msg))
		}
	}
}

func (postgres *Postgres) Close() error {
	return (*postgres.conn).Close()
}

func (postgres *Postgres) writeMessages(messages ...pgproto3.Message) {
	var buf []byte
	var err error
	for _, message := range messages {
		buf, err = message.Encode(buf)
		PanicIfError(err, "Error encoding messages")
	}
	_, err = (*postgres.conn).Write(buf)
	PanicIfError(err, "Error writing messages")
}

func (postgres *Postgres) writeError(message string) {
	postgres.writeMessages(
		&pgproto3.ErrorResponse{Message: message},
		&pgproto3.ReadyForQuery{TxStatus: 'I'},
	)
}

func (postgres *Postgres) handleStartup() {
	startupMessage, err := postgres.backend.ReceiveStartupMessage()
	PanicIfError(err, "Error receiving startup message")

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.(*pgproto3.StartupMessage).Parameters

		if params["database"] != postgres.config.Database {
			postgres.writeError("database " + params["database"] + " does not exist")
			return
		}

		postgres.writeMessages(
			&pgproto3.AuthenticationOk{},
			&pgproto3.ReadyForQuery{TxStatus: 'I'},
		)
	case *pgproto3.SSLRequest:
		_, err = (*postgres.conn).Write([]byte("N"))
		PanicIfError(err, "Error sending deny SSL request")
		postgres.handleStartup()
	default:
		PanicIfError(fmt.Errorf("Unknown startup message: %#v", startupMessage))
	}
}
