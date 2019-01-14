package saga

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

type SagaManager struct {
	bus         gbus.Bus
	sagaDefs    []*SagaDef
	lock        *sync.Mutex
	instances   map[*SagaDef][]*SagaInstance
	msgToDefMap map[string][]*SagaDef
	sagaStore   SagaStore
}

func (imsm *SagaManager) isSagaAlreadyRegistered(sagaType reflect.Type) bool {
	for _, def := range imsm.sagaDefs {
		if def.sagaType == sagaType {
			return true
		}
	}
	return false
}

func (imsm *SagaManager) RegisterSaga(saga gbus.Saga) error {

	sagaType := reflect.TypeOf(saga)

	if imsm.isSagaAlreadyRegistered(sagaType) {
		return errors.New(fmt.Sprintf("Saga of type %v already registered.\n", sagaType))
	}

	def := &SagaDef{
		bus:            imsm.bus,
		sagaType:       sagaType,
		startedBy:      gbus.GetFqns(saga.StartedBy()),
		handlersFunMap: make(map[string]string),
		lock:           &sync.Mutex{},
		msgHandler:     imsm.handler}

	saga.RegisterAllHandlers(def)

	imsm.sagaDefs = append(imsm.sagaDefs, def)
	msgNames := def.getHandledMessages()

	for _, msgName := range msgNames {
		defs := imsm.msgToDefMap[msgName]
		if defs == nil {
			defs = make([]*SagaDef, 0)

		}
		defs = append(defs, def)
		imsm.msgToDefMap[msgName] = defs
	}
	return nil
}

func (imsm *SagaManager) newSagaInstance(def *SagaDef) *SagaInstance {
	newInstance := def.newInstance()
	imsm.saveNewSaga(def, newInstance)

	return newInstance
}

func (imsm *SagaManager) saveNewSaga(def *SagaDef, newInstance *SagaInstance) error {
	instances := imsm.instances[def]
	if instances == nil {
		instances = make([]*SagaInstance, 0)

	}
	instances = append(instances, newInstance)
	imsm.instances[def] = instances
	return nil

}

func (imsm *SagaManager) getSagaInstanceByID(sagaID string) (*SagaInstance, error) {

	for _, instances := range imsm.instances {
		for _, instance := range instances {
			if instance.ID == sagaID {
				return instance, nil
			}
		}
	}
	return nil, errors.New("no saga found for provided id")
}

func (imsm *SagaManager) handler(invocation gbus.Invocation, message *gbus.BusMessage) {
	imsm.lock.Lock()
	defer imsm.lock.Unlock()

	msgName := gbus.GetFqn(message.Payload)
	defs := imsm.msgToDefMap[msgName]

	for _, def := range defs {

		/*
			1) If SagaDef does not have handlers for the message type then log a warning (as this should not happen) and return
			2) Else if the message is a startup message then create new instance of a saga, invoke startup handler and mark as started
			3) Else if message is destinated for a specific saga instance (reply messages) then find that saga by id and invoke it
			4) Else if message is not an event drop it (cmd messages should have 1 specific target)
			5) Else iterate over all instances and invoke the needed handler
		*/
		startNew := def.shouldStartNewSaga(message)
		if startNew {
			newInstance := imsm.newSagaInstance(def)
			newInstance.invoke(invocation, message)
			if !newInstance.isComplete() {
				if e := imsm.sagaStore.SaveNewSaga(invocation.Tx(), def, newInstance); e != nil {
					panic(e)
				}
			}

		} else if message.SagaCorrelationID != "" {
			instance, e := imsm.sagaStore.GetSagaByID(invocation.Tx(), message.SagaCorrelationID)
			if e != nil {
				panic(e)
			}
			if instance == nil {
				log.Printf("Warning:Failed message routed with SagaCorrelationID:%v but no saga instance with the same id found ", message.SagaCorrelationID)
				return
			}
			instance.invoke(invocation, message)
			if instance.isComplete() {
				e := imsm.sagaStore.DeleteSaga(invocation.Tx(), instance)
				if e != nil {
					panic(e)
				}
			} else {
				e := imsm.sagaStore.UpdateSaga(invocation.Tx(), instance)
				if e != nil {
					panic(e)
				}
			}
		} else if message.Semantics == "cmd" {
			log.Printf("Warning:Command or Reply message with no saga reference recieved. message will be dropped.\nmessage as of type:%v", reflect.TypeOf(message).Name())
			return
		} else {
			for _, instance := range imsm.instances[def] {
				instance.invoke(invocation, message)
				e := imsm.sagaStore.UpdateSaga(invocation.Tx(), instance)
				if e != nil {
					panic(e)
				}
			}
		}
	}
}

func NewSagaManager(bus gbus.Bus, sagaStore SagaStore) *SagaManager {
	return &SagaManager{
		bus:         bus,
		sagaDefs:    make([]*SagaDef, 0),
		lock:        &sync.Mutex{},
		instances:   make(map[*SagaDef][]*SagaInstance),
		msgToDefMap: make(map[string][]*SagaDef),
		sagaStore:   sagaStore}
}
