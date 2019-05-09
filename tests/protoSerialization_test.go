package tests

import (
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus/serialization"
)

func TestProtoSerialization(t *testing.T) {

	logger := log.WithField("test", "proto_serialization")
	serializer := serialization.NewProtoSerializer(logger)
	cmd := &ProtoCommand{}
	schemaName := cmd.SchemaName()
	cmd.SomeNumber = 15
	cmd.SomeData = "rhinof"
	encodedBytes, encErr := serializer.Encode(cmd)

	if encErr != nil {
		t.Errorf("encoding returned an error: %v", encErr)
	}

	//Calling Decode with out first registering the schema should fail and return an error
	_, decErr := serializer.Decode(encodedBytes, schemaName)
	if decErr == nil {
		t.Error("decoding expected to fail but did not return an error")
	}

	serializer.Register(cmd)
	decodedMsg, noErr := serializer.Decode(encodedBytes, schemaName)
	if noErr != nil {
		t.Errorf("decoding of message failed with error:%v", noErr)
	}

	cmdCopy, ok := decodedMsg.(*ProtoCommand)

	if !ok {
		t.Errorf("decoded message was of wrong type. expected:%v actual:%v", reflect.TypeOf(cmd), reflect.TypeOf(decodedMsg))
	}

	if cmdCopy.SomeNumber != cmd.SomeNumber || cmdCopy.SomeData != cmd.SomeData {
		log.Infof("expected:%v\n", cmd)
		log.Infof("actual:%v\n", cmdCopy)
		t.Errorf("decoded message has unexpected or missing data")
	}

}
