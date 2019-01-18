package saga

import (
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

type SagaDef struct {
	bus            gbus.Bus
	sagaType       reflect.Type
	startedBy      []string
	lock           *sync.Mutex
	instances      []*SagaInstance
	handlersFunMap map[string]string
	msgHandler     gbus.MessageHandler
}

func (sd *SagaDef) HandleMessage(message interface{}, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(message, handler)
	return sd.bus.HandleMessage(message, sd.msgHandler)
}
func (sd *SagaDef) HandleEvent(exchange, topic string, event interface{}, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(event, handler)
	return sd.bus.HandleEvent(exchange, topic, event, sd.msgHandler)
}

func (sd *SagaDef) getHandledMessages() []string {
	messages := make([]string, 0)
	for msgName, _ := range sd.handlersFunMap {
		messages = append(messages, msgName)
	}
	return messages
}

func (sd *SagaDef) addMsgToHandlerMapping(message interface{}, handler gbus.MessageHandler) {
	msgName := gbus.GetFqn(message)
	funName := runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	sd.handlersFunMap[msgName] = fn
}

func (sd *SagaDef) newInstance() *SagaInstance {
	return newSagaInstance(sd.sagaType,
		sd.handlersFunMap)
}

func (sd *SagaDef) shouldStartNewSaga(message *gbus.BusMessage) bool {
	fqn := gbus.GetFqn(message.Payload)
	for _, starts := range sd.startedBy {
		if fqn == starts {
			return true
		}
	}
	return false
}

// func (sd *SagaDef) canTimeout() bool {
// 	sd.sagaType.Implements(u reflect.Type)
// }

func (sd *SagaDef) String() string {
	return gbus.GetTypeFQN(sd.sagaType)
}
