package main

import (
	cemirpc "github.com/kodernubie/cemi-rpc"
	"github.com/kodernubie/cemi-rpc/rabbitmq"
)

func main() {

	cemirpc.AddConn(rabbitmq.New(rabbitmq.Options{
		URL: "amqp://guest:guest@localhost:5673",
	}))

	cemirpc.Serve("func1", func(methodName string, params ...any) ([]any, error) {

		return nil, nil
	})

	select {}
}
