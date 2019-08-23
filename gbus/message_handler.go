package gbus

import (
	"database/sql"
	"reflect"
	"runtime"
	"strings"

	"github.com/streadway/amqp"
)

//MessageHandler signature for all command handlers
type MessageHandler func(invocation Invocation, message *BusMessage) error

//RawMessageHandler signature for handlers that handle raw amqp deliveries
type RawMessageHandler func(tx *sql.Tx, delivery *amqp.Delivery) error

//Name is a helper function returning the runtime name of the function bound to an instance of the MessageHandler type
func (mg MessageHandler) Name() string {
	return nameFromFunc(mg)
}

//Name is a helper function returning the runtime name of the function bound to an instance of the DeadLetterMessageHandler type
func (dlmg RawMessageHandler) Name() string {
	return nameFromFunc(dlmg)
}

func nameFromFunc(function interface{}) string {
	funName := runtime.FuncForPC(reflect.ValueOf(function).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	return fn
}
