// Copyright © 2019 Vladislav Shub <vladislav.shub@wework.com>
// All rights reserved to the We Company.

package serialization

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/wework/grabbit/gbus"
)

var _ gbus.Serializer = &Proto{}

//Proto a serializer for GBus uses protobuf
type Proto struct {
	lock              *sync.Mutex
	registeredSchemas map[string]reflect.Type
	logger            logrus.FieldLogger
}

//NewProtoSerializer creates a new instance of Proto and returns it
func NewProtoSerializer(logger logrus.FieldLogger) gbus.Serializer {
	return &Proto{
		registeredSchemas: make(map[string]reflect.Type),
		lock:              &sync.Mutex{},
		logger:            logger.WithField("serializer", "proto"),
	}
}

//Name implements Serializer.Name
func (as *Proto) Name() string {
	return "proto"
}

//Encode encodes an object into a byte array
func (as *Proto) Encode(obj gbus.Message) (buffer []byte, err error) {
	msg, ok := obj.(proto.Message)
	if !ok {
		err = fmt.Errorf("could not cast message into proto.Message")
		as.logger.WithError(err).Error("invalid type of object")
		return nil, err
	}
	buffer, err = proto.Marshal(msg)
	if err != nil {
		as.logger.WithError(err).Error("could not Marshal msg")
		return nil, err
	}
	return
}

//Decode decodes a byte array into an object
func (as *Proto) Decode(buffer []byte, schemaName string) (msg gbus.Message, err error) {
	t, ok := as.registeredSchemas[schemaName]
	if !ok {
		err = fmt.Errorf("could not find the message type in proto registry")
		as.logger.WithError(err).Error("unknown type")
		return nil, err
	}
	tmsg, ok := reflect.New(t.Elem()).Interface().(proto.Message)
	if !ok {
		err = fmt.Errorf("could not cast item to proto.Message")
		as.logger.WithError(err).Error("invalid object")
		return nil, err
	}
	err = proto.Unmarshal(buffer, tmsg)
	if err != nil {
		as.logger.WithError(err).Error("could not Unmarshal buffer to msg")
		return nil, err
	}

	msg, ok = tmsg.(gbus.Message)
	if !ok {
		err = fmt.Errorf("could not cast obj to gbus.Message")
		as.logger.WithError(err).WithField("msg", tmsg).Errorf("could not cast %v to gbus.Message", tmsg)
		return nil, err
	}

	return msg, nil
}

//Register proto messages so we can have lots of fun!
func (as *Proto) Register(obj gbus.Message) {
	as.lock.Lock()
	defer as.lock.Unlock()
	if as.registeredSchemas[obj.SchemaName()] == nil {
		as.logger.WithField("SchemaName", obj.SchemaName()).Debug("registering schema to proto")
		as.registeredSchemas[obj.SchemaName()] = reflect.TypeOf(obj)
	}
}
