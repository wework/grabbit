package gbus

import (
	"reflect"
	"runtime"
	"strings"
)

//MessageHandler signature for all command handlers
type MessageHandler func(invocation Invocation, message *BusMessage) error

//Name is a helper function returning the runtime name of the function bound to an instance of the MessageHandler type
func (mg MessageHandler) Name() string {
	funName := runtime.FuncForPC(reflect.ValueOf(mg).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	return fn
}
