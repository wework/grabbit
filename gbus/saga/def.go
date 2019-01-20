package saga

import (
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

//Def dwefines a saga type
type Def struct {
	bus            gbus.Bus
	sagaType       reflect.Type
	startedBy      []string
	lock           *sync.Mutex
	instances      []*Instance
	handlersFunMap map[string]string
	msgHandler     gbus.MessageHandler
}

//HandleMessage implements HandlerRegister interface
func (sd *Def) HandleMessage(message interface{}, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(message, handler)
	return sd.bus.HandleMessage(message, sd.msgHandler)
}

//HandleEvent implements HandlerRegister interface
func (sd *Def) HandleEvent(exchange, topic string, event interface{}, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(event, handler)
	return sd.bus.HandleEvent(exchange, topic, event, sd.msgHandler)
}

func (sd *Def) getHandledMessages() []string {
	messages := make([]string, 0)
	for msgName := range sd.handlersFunMap {
		messages = append(messages, msgName)
	}
	return messages
}

func (sd *Def) addMsgToHandlerMapping(message interface{}, handler gbus.MessageHandler) {
	msgName := gbus.GetFqn(message)
	funName := runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	sd.handlersFunMap[msgName] = fn
}

func (sd *Def) newInstance() *Instance {
	return newInstance(sd.sagaType,
		sd.handlersFunMap)
}

func (sd *Def) shouldStartNewSaga(message *gbus.BusMessage) bool {
	fqn := gbus.GetFqn(message.Payload)
	for _, starts := range sd.startedBy {
		if fqn == starts {
			return true
		}
	}
	return false
}

// func (sd *Def) canTimeout() bool {
// 	sd.sagaType.Implements(u reflect.Type)
// }

func (sd *Def) String() string {
	return gbus.GetTypeFQN(sd.sagaType)
}
