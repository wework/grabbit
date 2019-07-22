package gbus

import (
	"reflect"
	"runtime"
	"strings"
)

//MessageHandler signature for all command handlers
type MessageHandler func(invocation Invocation, message *BusMessage) error

func (mg MessageHandler) Name() string {
	funName := runtime.FuncForPC(reflect.ValueOf(mg).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	return fn
}
