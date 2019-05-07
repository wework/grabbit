package saga

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
)

func fqnsFromMessages(objs []gbus.Message) []string {
	fqns := make([]string, 0)
	for _, obj := range objs {
		fqn := obj.SchemaName()
		fqns = append(fqns, fqn)
	}
	return fqns
}

var _ gbus.SagaRegister = &Glue{}

//Glue ties the incoming messages from the Bus with the needed Saga instances
type Glue struct {
	svcName          string
	bus              gbus.Bus
	sagaDefs         []*Def
	lock             *sync.Mutex
	alreadyRegistred map[string]bool
	msgToDefMap      map[string][]*Def
	sagaStore        Store
	timeoutManger    TimeoutManager
}

func (imsm *Glue) isSagaAlreadyRegistered(sagaType reflect.Type) bool {
	for _, def := range imsm.sagaDefs {
		if def.sagaType == sagaType {
			return true
		}
	}
	return false
}

//RegisterSaga registers the saga instance with the Bus
func (imsm *Glue) RegisterSaga(saga gbus.Saga, conf ...gbus.SagaConfFn) error {

	sagaType := reflect.TypeOf(saga)

	if imsm.isSagaAlreadyRegistered(sagaType) {
		return fmt.Errorf("saga of type %v already registered", sagaType)
	}

	imsm.sagaStore.RegisterSagaType(saga)

	def := &Def{

		glue:        imsm,
		sagaType:    sagaType,
		sagaConfFns: conf,
		startedBy:   fqnsFromMessages(saga.StartedBy()),
		msgToFunc:   make([]*MsgToFuncPair, 0),
		lock:        &sync.Mutex{}}

	saga.RegisterAllHandlers(def)
	imsm.sagaDefs = append(imsm.sagaDefs, def)
	msgNames := def.getHandledMessages()

	for _, msgName := range msgNames {
		imsm.addMsgNameToDef(msgName, def)
	}

	imsm.log().
		WithFields(log.Fields{"saga_type": def.sagaType.String(), "handles_messages": len(msgNames)}).
		Info("registered saga with messages")

	//register on timeout messages
	timeoutEtfs, requestsTimeout := saga.(gbus.RequestSagaTimeout)
	if requestsTimeout {
		timeoutMessage := gbus.SagaTimeoutMessage{}
		timeoutMsgName := timeoutMessage.SchemaName()
		_ = def.HandleMessage(timeoutMessage, timeoutEtfs.Timeout)
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

func (imsm *Glue) handler(invocation gbus.Invocation, message *gbus.BusMessage) error {

	imsm.lock.Lock()
	defer imsm.lock.Unlock()
	msgName := message.PayloadFQN

	defs := imsm.msgToDefMap[strings.ToLower(msgName)]

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
			newInstance := def.newInstance()
			imsm.log().
				WithFields(log.Fields{"saga_def": def.String(), "saga_id": newInstance.ID}).
				Info("created new saga")
			if invkErr := imsm.invokeSagaInstance(newInstance, invocation, message); invkErr != nil {
				imsm.log().WithError(invkErr).WithField("saga_id", newInstance.ID).Error("failed to invoke saga")
				return invkErr
			}

			if !newInstance.isComplete() {
				imsm.log().WithField("saga_id", newInstance.ID).Info("saving new saga")

				if e := imsm.sagaStore.SaveNewSaga(invocation.Tx(), def.sagaType, newInstance); e != nil {
					imsm.log().WithError(e).WithField("saga_id", newInstance.ID).Error("saving new saga failed")
					return e
				}

				if requestsTimeout, duration := newInstance.requestsTimeout(); requestsTimeout {
					imsm.log().WithFields(log.Fields{"saga_id": newInstance.ID, "timeout_duration": duration}).Info("new saga requested timeout")
					imsm.timeoutManger.RequestTimeout(imsm.svcName, newInstance.ID, duration)
				}
			}
			return nil
		} else if message.SagaCorrelationID != "" {
			instance, getErr := imsm.sagaStore.GetSagaByID(invocation.Tx(), message.SagaCorrelationID)

			if getErr != nil {
				imsm.log().WithError(getErr).WithField("saga_id", message.SagaCorrelationID).Error("failed to fetch saga by id")
				return getErr
			}
			if instance == nil {
				e := fmt.Errorf("Warning:Failed message routed with SagaCorrelationID:%v but no saga instance with the same id found ", message.SagaCorrelationID)
				return e
			}

			if invkErr := imsm.invokeSagaInstance(instance, invocation, message); invkErr != nil {
				imsm.log().WithError(invkErr).WithField("saga_id", instance.ID).Error("failed to invoke saga")
				return invkErr
			}

			return imsm.completeOrUpdateSaga(invocation.Tx(), instance, message)

		} else if message.Semantics == gbus.CMD {
			e := fmt.Errorf("Warning:Command or Reply message with no saga reference received. message will be dropped.\nmessage as of type:%v", reflect.TypeOf(message).Name())
			return e
		} else {

			imsm.log().WithFields(log.Fields{"saga_type": def.sagaType, "message": msgName}).Info("fetching saga instances by type")
			instances, e := imsm.sagaStore.GetSagasByType(invocation.Tx(), def.sagaType)

			if e != nil {
				return e
			}
			imsm.log().WithFields(log.Fields{"message": msgName, "instances_fetched": len(instances)}).Info("fetched saga instances")

			for _, instance := range instances {

				if invkErr := imsm.invokeSagaInstance(instance, invocation, message); invkErr != nil {
					imsm.log().WithError(invkErr).WithField("saga_id", instance.ID).Error("failed to invoke saga")
					return invkErr
				}
				e = imsm.completeOrUpdateSaga(invocation.Tx(), instance, message)
				if e != nil {
					return e
				}
			}
		}
	}

	return nil
}

func (imsm *Glue) invokeSagaInstance(instance *Instance, invocation gbus.Invocation, message *gbus.BusMessage) error {
	sginv := &sagaInvocation{
		decoratedBus:        invocation.Bus(),
		decoratedInvocation: invocation,
		inboundMsg:          message,
		sagaID:              instance.ID,
		ctx:                 invocation.Ctx(),
		invokingService:     imsm.svcName}

	exchange, routingKey := invocation.Routing()
	return instance.invoke(exchange, routingKey, sginv, message)
}

func (imsm *Glue) completeOrUpdateSaga(tx *sql.Tx, instance *Instance, lastMessage *gbus.BusMessage) error {

	_, timedOut := lastMessage.Payload.(gbus.SagaTimeoutMessage)

	if instance.isComplete() || timedOut {
		imsm.log().WithField("saga_id", instance.ID).Info("saga has completed and will be deleted")

		return imsm.sagaStore.DeleteSaga(tx, instance)

	}
	return imsm.sagaStore.UpdateSaga(tx, instance)
}

func (imsm *Glue) registerMessage(message gbus.Message) error {
	//only register once on each message so we will not duplicate invocations
	if _, exists := imsm.alreadyRegistred[message.SchemaName()]; exists {
		return nil
	}
	imsm.alreadyRegistred[message.SchemaName()] = true
	return imsm.bus.HandleMessage(message, imsm.handler)
}

func (imsm *Glue) registerEvent(exchange, topic string, event gbus.Message) error {

	if _, exists := imsm.alreadyRegistred[event.SchemaName()]; exists {
		return nil
	}
	imsm.alreadyRegistred[event.SchemaName()] = true
	return imsm.bus.HandleEvent(exchange, topic, event, imsm.handler)
}

func (imsm *Glue) log() *log.Entry {
	return log.WithField("_service", imsm.svcName)
}

//NewGlue creates a new Sagamanager
func NewGlue(bus gbus.Bus, sagaStore Store, svcName string) *Glue {
	return &Glue{
		svcName:          svcName,
		bus:              bus,
		sagaDefs:         make([]*Def, 0),
		lock:             &sync.Mutex{},
		alreadyRegistred: make(map[string]bool),
		msgToDefMap:      make(map[string][]*Def),
		timeoutManger:    TimeoutManager{bus: bus},
		sagaStore:        sagaStore,
	}
}
