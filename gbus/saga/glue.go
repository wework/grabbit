package saga

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

type Glue struct {
	svcName              string
	bus                  gbus.Bus
	sagaDefs             []*Def
	lock                 *sync.Mutex
	instances            map[*Def][]*Instance
	msgToDefMap          map[string][]*Def
	sagaStore            Store
	timeoutManger        TimeoutManager
	subscribedOnTimeouts bool
}

func (imsm *Glue) isSagaAlreadyRegistered(sagaType reflect.Type) bool {
	for _, def := range imsm.sagaDefs {
		if def.sagaType == sagaType {
			return true
		}
	}
	return false
}

//RegisterSaga
func (imsm *Glue) RegisterSaga(saga gbus.Saga) error {

	sagaType := reflect.TypeOf(saga)

	if imsm.isSagaAlreadyRegistered(sagaType) {
		return fmt.Errorf("saga of type %v already registered", sagaType)
	}

	def := &Def{
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
		imsm.addMsgNameToDef(msgName, def)
	}

	//register on timeout messages
	timeoutEtfs, requestsTimeout := saga.(gbus.RequestSagaTimeout)
	if requestsTimeout {
		timeoutMessage := gbus.SagaTimeoutMessage{}
		timeoutMsgName := gbus.GetFqn(timeoutMessage)
		def.HandleMessage(timeoutMessage, timeoutEtfs.Timeout)
		// def.addMsgToHandlerMapping(timeoutMessage, timeoutEtfs.Timeout)
		imsm.addMsgNameToDef(timeoutMsgName, def)

	}
	return nil
}

func (imsm *Glue) addMsgNameToDef(msgName string, def *Def) {
	defs := imsm.getDefsForMsgName(msgName)
	defs = append(defs, def)
	imsm.msgToDefMap[msgName] = defs
}

func (imsm *Glue) getDefsForMsgName(msgName string) []*Def {
	defs := imsm.msgToDefMap[msgName]
	if defs == nil {
		defs = make([]*Def, 0)
	}
	return defs
}

func (imsm *Glue) newInstance(def *Def) *Instance {
	newInstance := def.newInstance()
	imsm.saveNewSaga(def, newInstance)

	return newInstance
}

func (imsm *Glue) saveNewSaga(def *Def, newInstance *Instance) error {
	instances := imsm.instances[def]
	if instances == nil {
		instances = make([]*Instance, 0)

	}
	instances = append(instances, newInstance)
	imsm.instances[def] = instances
	return nil

}

func (imsm *Glue) getInstanceByID(sagaID string) (*Instance, error) {

	for _, instances := range imsm.instances {
		for _, instance := range instances {
			if instance.ID == sagaID {
				return instance, nil
			}
		}
	}
	return nil, errors.New("no saga found for provided id")
}

func (imsm *Glue) handler(invocation gbus.Invocation, message *gbus.BusMessage) {
	imsm.lock.Lock()
	defer imsm.lock.Unlock()

	msgName := gbus.GetFqn(message.Payload)
	defs := imsm.msgToDefMap[msgName]

	for _, def := range defs {
		/*
			1) If Def does not have handlers for the message type then log a warning (as this should not happen) and return
			2) Else if the message is a startup message then create new instance of a saga, invoke startup handler and mark as started
				2.1) If new instance requests timeouts then reuqest a timeout
			3) Else if message is destinated for a specific saga instance (reply messages) then find that saga by id and invoke it
			4) Else if message is not an event drop it (cmd messages should have 1 specific target)
			5) Else iterate over all instances and invoke the needed handler
		*/
		startNew := def.shouldStartNewSaga(message)
		if startNew {
			newInstance := imsm.newInstance(def)
			log.Printf("created new saga.\nSaga Def:%v\nSagaID:%v", def.String(), newInstance.ID)
			newInstance.invoke(invocation, message)

			if !newInstance.isComplete() {
				log.Printf("saving new saga with sagaID %v", newInstance.ID)
				if e := imsm.sagaStore.SaveNewSaga(invocation.Tx(), def, newInstance); e != nil {
					log.Printf("saving new saga failed\nSagaID:%v", newInstance.ID)
					panic(e)
				}

				if requestsTimeout, duration := newInstance.requestsTimeout(); requestsTimeout == true {
					log.Printf("new saga requested timeout\nTimeout duration:%v", duration)
					imsm.timeoutManger.RequestTimeout(imsm.svcName, newInstance.ID, duration)
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
			e = imsm.completeOrUpdateSaga(invocation.Tx(), instance)
			if e != nil {
				panic(e)
			}
		} else if message.Semantics == "cmd" {
			log.Printf("Warning:Command or Reply message with no saga reference received. message will be dropped.\nmessage as of type:%v", reflect.TypeOf(message).Name())
			return
		} else {
			for _, instance := range imsm.instances[def] {
				instance.invoke(invocation, message)
				e := imsm.completeOrUpdateSaga(invocation.Tx(), instance)
				if e != nil {
					panic(e)
				}
			}
		}
	}
}

func (imsm *Glue) completeOrUpdateSaga(tx *sql.Tx, instance *Instance) error {
	if instance.isComplete() {
		log.Printf("sage %v has completed and will be deleted", instance.ID)
		return imsm.sagaStore.DeleteSaga(tx, instance)

	}
	return imsm.sagaStore.UpdateSaga(tx, instance)
}

//NewGlue creates a new Sagamanager
func NewGlue(bus gbus.Bus, sagaStore Store, svcName string) *Glue {
	return &Glue{
		svcName:       svcName,
		bus:           bus,
		sagaDefs:      make([]*Def, 0),
		lock:          &sync.Mutex{},
		instances:     make(map[*Def][]*Instance),
		msgToDefMap:   make(map[string][]*Def),
		timeoutManger: TimeoutManager{bus: bus},
		sagaStore:     sagaStore}
}
