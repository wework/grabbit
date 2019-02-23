package saga

import (
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

//Def defines a saga type
type Def struct {
	glue           *Glue
	sagaType       reflect.Type
	startedBy      []string
	lock           *sync.Mutex
	instances      []*Instance
	handlersFunMap map[string]string
	msgHandler     gbus.MessageHandler
}

//HandleMessage implements HandlerRegister interface
func (sd *Def) HandleMessage(message gbus.Message, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(message, handler)
	return sd.glue.registerMessage(message)
}

//HandleEvent implements HandlerRegister interface
func (sd *Def) HandleEvent(exchange, topic string, event gbus.Message, handler gbus.MessageHandler) error {
	sd.addMsgToHandlerMapping(event, handler)
	return sd.glue.registerEvent(exchange, topic, event)
}

func (sd *Def) getHandledMessages() []string {
	messages := make([]string, 0)
	for msgName := range sd.handlersFunMap {
		messages = append(messages, msgName)
	}
	return messages
}

func (sd *Def) addMsgToHandlerMapping(message gbus.Message, handler gbus.MessageHandler) {
	msgName := message.SchemaName()
	funName := runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
	splits := strings.Split(funName, ".")
	fn := strings.Replace(splits[len(splits)-1], "-fm", "", -1)
	sd.handlersFunMap[msgName] = fn
}

func (sd *Def) newInstance() *Instance {
	return NewInstance(sd.sagaType,
		sd.handlersFunMap)
}

func (sd *Def) shouldStartNewSaga(message *gbus.BusMessage) bool {
	fqn := message.PayloadFQN

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
