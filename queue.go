package cemirpc

import (
	"sync"
)

var mutex sync.RWMutex
var connections map[string]Connection = map[string]Connection{}
var builders map[string]ConnectionBuilder = map[string]ConnectionBuilder{}

type SubscribeHandler func(subject, payload string) bool
type ConnectionBuilder func(name string) (Connection, error)

type Connection interface {
	Publish(subject, route string, payload string) error
	Subscribe(subject string, route []string, handler SubscribeHandler, workerNo ...int) error
	QueueSubscribe(subject, group string, route []string, handler SubscribeHandler, workerNo ...int) error
	Call(methodName string, params ...any) Result
	Serve(methodName string, handler ServeHandler, workerNo ...int) error
}

func AddConn(conn Connection, name ...string) {

	targetName := ""

	if len(name) > 0 {
		targetName = name[0]
	}

	mutex.Lock()
	defer mutex.Unlock()

	connections[targetName] = conn
}

func Get(name ...string) Connection {

	targetName := ""

	if len(name) > 0 {
		targetName = name[0]
	}

	mutex.RLock()
	defer mutex.RUnlock()

	return connections[targetName]
}
