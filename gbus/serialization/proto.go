// Copyright © 2019 Vladislav Shub <vladislav.shub@wework.com>
// All rights reserved to the We Company.

package serialization

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"reflect"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

var _ gbus.Serializer = &Proto{}

//Proto a serializer for GBus uses protobuf
type Proto struct {
	lock              *sync.Mutex
	registeredSchemas map[string]reflect.Type
}

//NewProtoSerializer creates a new instance of Proto and returns it
func NewProtoSerializer() gbus.Serializer {
	return &Proto{
		registeredSchemas: make(map[string]reflect.Type),
		lock:              &sync.Mutex{},
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
		return nil, fmt.Errorf("could not cast message into proto.Message")
	}
	buffer, err = proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return
}

//Decode decodes a byte array into an object
func (as *Proto) Decode(buffer []byte, schemaName string) (gbus.Message, error) {
	t, ok := as.registeredSchemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("could not find the message type in proto registry")
	}
	msg, ok := reflect.New(t.Elem()).Interface().(proto.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast item to proto.Message")
	}
	err := proto.Unmarshal(buffer, msg)
	if err != nil {
		return nil, err
	}

	gmsg, ok := msg.(gbus.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to gbus.Message", msg)
	}

	return gmsg, nil
}

//Register proto messages so we can have lots of fun!
func (as *Proto) Register(obj gbus.Message) {
	as.lock.Lock()
	defer as.lock.Unlock()
	if as.registeredSchemas[obj.SchemaName()] == nil {
		as.registeredSchemas[obj.SchemaName()] = reflect.TypeOf(obj)
	}
}
