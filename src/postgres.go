package main

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	PG_VERSION        = "17.0"
	PG_ENCODING       = "UTF8"
	PG_TX_STATUS_IDLE = 'I'
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
		message, err := postgres.backend.Receive()
		if err != nil {
			return // Terminate connection
		}

		switch message.(type) {
		case *pgproto3.Query:
			postgres.handleSimpleQuery(proxy, message.(*pgproto3.Query))
		case *pgproto3.Parse:
			err = postgres.handleExtendedQuery(proxy, message.(*pgproto3.Parse))
			if err != nil {
				return // Terminate connection
			}
		case *pgproto3.Terminate:
			LogDebug(postgres.config, "Client terminated connection")
			return
		default:
			PanicIfError(fmt.Errorf("Received message other than Query from client: %#v", message))
		}
	}
}

func (postgres *Postgres) Close() error {
	return (*postgres.conn).Close()
}

func (postgres *Postgres) handleSimpleQuery(proxy *Proxy, queryMessage *pgproto3.Query) {
	LogDebug(postgres.config, "Received query:", queryMessage.String)
	messages, err := proxy.HandleQuery(queryMessage.String)
	if err != nil {
		postgres.writeError("Internal error")
		return
	}
	messages = append(messages, &pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE})
	postgres.writeMessages(messages...)
}

func (postgres *Postgres) handleExtendedQuery(proxy *Proxy, parseMessage *pgproto3.Parse) error {
	LogDebug(postgres.config, "Parsing query", parseMessage.Query)
	messages, preparedStatement, err := proxy.HandleParseQuery(parseMessage)
	if err != nil {
		postgres.writeError("Failed to parse query")
		return nil
	}
	postgres.writeMessages(messages...)

	for {
		message, err := postgres.backend.Receive()
		if err != nil {
			return err
		}

		switch message.(type) {
		case *pgproto3.Bind:
			message := message.(*pgproto3.Bind)
			LogDebug(postgres.config, "Binding query", message.PreparedStatement)
			messages, preparedStatement, err = proxy.HandleBindQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError("Failed to bind query")
				continue
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Describe:
			message := message.(*pgproto3.Describe)
			LogDebug(postgres.config, "Describing query", message.Name, "("+string(message.ObjectType)+")")
			var messages []pgproto3.Message
			messages, preparedStatement, err = proxy.HandleDescribeQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError("Failed to describe query")
				continue
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Execute:
			message := message.(*pgproto3.Execute)
			LogDebug(postgres.config, "Executing query", message.Portal)
			messages, err := proxy.HandleExecuteQuery(message, preparedStatement)
			if err != nil {
				postgres.writeError("Failed to execute query")
				continue
			}
			postgres.writeMessages(messages...)
		case *pgproto3.Sync:
			LogDebug(postgres.config, "Syncing query")
			postgres.writeMessages(
				&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
			)
			return nil
		}
	}
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
		&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
	)
}

func (postgres *Postgres) handleStartup() {
	startupMessage, err := postgres.backend.ReceiveStartupMessage()
	PanicIfError(err, "Error receiving startup message")

	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		params := startupMessage.(*pgproto3.StartupMessage).Parameters
		LogDebug(postgres.config, "BemiDB: startup message", params)

		if params["database"] != postgres.config.Database {
			postgres.writeError("database " + params["database"] + " does not exist")
			return
		}

		postgres.writeMessages(
			&pgproto3.AuthenticationOk{},
			&pgproto3.ParameterStatus{Name: "client_encoding", Value: PG_ENCODING},
			&pgproto3.ParameterStatus{Name: "server_version", Value: PG_VERSION},
			&pgproto3.ReadyForQuery{TxStatus: PG_TX_STATUS_IDLE},
		)
	case *pgproto3.SSLRequest:
		_, err = (*postgres.conn).Write([]byte("N"))
		PanicIfError(err, "Error sending deny SSL request")
		postgres.handleStartup()
	default:
		PanicIfError(fmt.Errorf("Unknown startup message: %#v", startupMessage))
	}
}
