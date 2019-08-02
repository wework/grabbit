package saga

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
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

//ErrInstanceNotFound is returned by the saga store if a saga lookup by saga id returns no valid instances
var ErrInstanceNotFound = errors.New("saga  not be found")

var _ gbus.SagaGlue = &Glue{}

//Glue t/*  */ies the incoming messages from the Bus with the needed Saga instances
type Glue struct {
	svcName          string
	bus              gbus.Bus
	sagaDefs         []*Def
	lock             *sync.Mutex
	alreadyRegistred map[string]bool
	msgToDefMap      map[string][]*Def
	sagaStore        Store
	getLog           func() logrus.FieldLogger
	timeoutManager   gbus.TimeoutManager
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
		WithFields(logrus.Fields{"saga_type": def.sagaType.String(), "handles_messages": len(msgNames)}).
		Info("registered saga with messages")

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
				WithFields(logrus.Fields{"saga_def": def.String(), "saga_id": newInstance.ID}).
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
					imsm.log().WithFields(logrus.Fields{"saga_id": newInstance.ID, "timeout_duration": duration}).Info("new saga requested timeout")
					if tme := imsm.timeoutManager.RegisterTimeout(invocation.Tx(), newInstance.ID, duration); tme != nil {
						return tme
					}
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
			def.configureSaga(instance)
			if invkErr := imsm.invokeSagaInstance(instance, invocation, message); invkErr != nil {
				imsm.log().WithError(invkErr).WithField("saga_id", instance.ID).Error("failed to invoke saga")
				return invkErr
			}

			return imsm.completeOrUpdateSaga(invocation.Tx(), instance)

		} else if message.Semantics == gbus.CMD {
			e := fmt.Errorf("Warning:Command or Reply message with no saga reference received. message will be dropped.\nmessage as of type:%v", reflect.TypeOf(message).Name())
			return e
		} else {

			imsm.log().WithFields(logrus.Fields{"saga_type": def.sagaType, "message": msgName}).Info("fetching saga instances by type")
			instances, e := imsm.sagaStore.GetSagasByType(invocation.Tx(), def.sagaType)

			if e != nil {
				return e
			}
			imsm.log().WithFields(logrus.Fields{"message": msgName, "instances_fetched": len(instances)}).Info("fetched saga instances")

			for _, instance := range instances {
				def.configureSaga(instance)
				if invkErr := imsm.invokeSagaInstance(instance, invocation, message); invkErr != nil {
					imsm.log().WithError(invkErr).WithField("saga_id", instance.ID).Error("failed to invoke saga")
					return invkErr
				}
				e = imsm.completeOrUpdateSaga(invocation.Tx(), instance)
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
		invokingService:     imsm.svcName,
	}
	sginv.SetLogger(imsm.log().WithFields(logrus.Fields{
		"saga_id":      instance.ID,
		"saga_type":    instance.String(),
		"message_name": message.PayloadFQN,
	}))

	exchange, routingKey := invocation.Routing()
	return instance.invoke(exchange, routingKey, sginv, message)
}

func (imsm *Glue) completeOrUpdateSaga(tx *sql.Tx, instance *Instance) error {

	if instance.isComplete() {
		imsm.log().WithField("saga_id", instance.ID).Info("saga has completed and will be deleted")

		deleteErr := imsm.sagaStore.DeleteSaga(tx, instance)
		if deleteErr != nil {
			return deleteErr
		}

		return imsm.timeoutManager.ClearTimeout(tx, instance.ID)

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

//TimeoutSaga fetches a saga instance and calls its timeout interface
func (imsm *Glue) TimeoutSaga(tx *sql.Tx, sagaID string) error {

	saga, err := imsm.sagaStore.GetSagaByID(tx, sagaID)
	//we are assuming that if the TimeoutSaga has been called but no instance returned from the store the saga
	//has been completed already and
	if err == ErrInstanceNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	timeoutErr := saga.timeout(tx, imsm.bus)
	if timeoutErr != nil {
		imsm.log().WithError(timeoutErr).WithField("sagaID", sagaID).Error("failed to timeout saga")
		return timeoutErr
	}
	return imsm.completeOrUpdateSaga(tx, saga)
}

func (imsm *Glue) log() logrus.FieldLogger {
	return imsm.getLog()
}

//Start starts the glue instance up
func (imsm *Glue) Start() error {
	return imsm.timeoutManager.Start()
}

//Stop starts the glue instance up
func (imsm *Glue) Stop() error {
	return imsm.timeoutManager.Stop()
}

//NewGlue creates a new Sagamanager
func NewGlue(bus gbus.Bus, sagaStore Store, svcName string, txp gbus.TxProvider, getLog func() logrus.FieldLogger, timeoutManager gbus.TimeoutManager) *Glue {
	g := &Glue{
		svcName:          svcName,
		bus:              bus,
		sagaDefs:         make([]*Def, 0),
		lock:             &sync.Mutex{},
		alreadyRegistred: make(map[string]bool),
		msgToDefMap:      make(map[string][]*Def),
		sagaStore:        sagaStore,
		getLog:           getLog,
		timeoutManager:   timeoutManager,
	}

	timeoutManager.AcceptTimeoutFunction(g.TimeoutSaga)
	return g
}
