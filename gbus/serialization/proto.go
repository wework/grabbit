// Copyright © 2019 Vladislav Shub <vladislav.shub@wework.com>
// All rights reserved to the We Company.

package serialization

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

var _ gbus.MessageEncoding = &ProtoSerializer{}

//ProtoSerializer a serializer for GBus uses protoubf
type ProtoSerializer struct {
	lock                 *sync.Mutex
	//registeredSchemas    map[string]*avroRelation
}

//NewProtoSerializer creates a new instance of ProtoSerializer and returns it
func NewProtoSerializer() gbus.MessageEncoding {
	return &ProtoSerializer{
		//registeredSchemas:    make(map[string]*avroRelation),
		lock:                 &sync.Mutex{},
	}
}

//EncoderID implements MessageEncoding.EncoderID
func (as *ProtoSerializer) EncoderID() string {
	return "proto"
}

//Encode encodes an object into a byte array
func (as *ProtoSerializer) Encode(obj gbus.Message) (msg []byte, err error) {
	pmsg, ok := obj.(*protoMessage)
	if !ok {
		return nil, fmt.Errorf("could not convert message into protoMessage")
	}
	return pmsg.buffer, nil
}

//Decode decodes a byte array into an object
func (as *ProtoSerializer) Decode(buffer []byte) (obj gbus.Message, err error) {
	return &protoMessage{
		buffer: buffer,
	}, nil
}

//Register not really used here :(
func (as *ProtoSerializer) Register(obj gbus.Message) {

	// TODO: we should think what is the best way to do this
}

var _ gbus.Message = &protoMessage{}
type protoMessage struct {
	buffer []byte
	msg proto.Message
	name string
}

func (pm *protoMessage) SchemaName() string {
	return pm.name
}

//CastMessageToProto converts a gbus.Message into a proto.Message if fails returns an error
func CastMessageToProto(obj gbus.Message, message proto.Message) error {
	msg, ok := obj.(*protoMessage)
	if !ok {
		return fmt.Errorf("could not cast message to protoMessage")
	}
	return proto.Unmarshal(msg.buffer, message)
}

//CastProtoToMessage converts a proto.Message into a gbus.Message
func CastProtoToMessage(message proto.Message) gbus.Message {
	return &protoMessage{
		msg: message,
		name: proto.MessageName(message),
	}
}

//CastProtoToBusMessage converts a proto.Message into a gbus.BusMessage
func CastProtoToBusMessage(message proto.Message) *gbus.BusMessage {
	return gbus.NewBusMessage(CastProtoToMessage(message))
}