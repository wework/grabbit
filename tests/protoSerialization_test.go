package tests

import (
	"crypto/rand"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus/serialization"
)

func getProtoCommand() *ProtoCommand {
	return &ProtoCommand{
		SomeNumber: 15,
		SomeData:   "rhinof"}
}

func TestProtoSerialization(t *testing.T) {
	// so the tests logs do not get littered with log entries from serlializer
	log.SetLevel(log.PanicLevel)
	defer log.SetLevel(log.InfoLevel)
	logger := log.WithField("test", "proto_serialization")

	serializer := serialization.NewProtoSerializer(logger)

	name := serializer.Name()
	if name != serialization.ProtoContentType {
		t.Fatalf("incorrect serializer name. expected:application/x-protobuf actual:%s", name)
	}
	cmd := getProtoCommand()
	schemaName := cmd.SchemaName()

	encodedBytes, encErr := serializer.Encode(cmd)
	if encErr != nil {
		t.Fatalf("encoding returned an error: %v", encErr)
	}

	//Calling Decode with out first registering the schema should fail and return an error
	_, decErr := serializer.Decode(encodedBytes, schemaName)
	if decErr == nil {
		t.Fatalf("decoding expected to fail but did not return an error")
	}

	serializer.Register(cmd)

	decodedMsg, noErr := serializer.Decode(encodedBytes, schemaName)
	if noErr != nil {
		t.Fatalf("decoding of message failed with error:%v", noErr)
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

func TestProtoSerializationErrors(t *testing.T) {
	// so the tests logs do not get littered with log entries from serlializer
	log.SetLevel(log.PanicLevel)
	defer log.SetLevel(log.InfoLevel)
	logger := log.WithField("test", "proto_serialization")

	serializer := serialization.NewProtoSerializer(logger)

	// test that encoding a non protobuf generated strcut fails and returns an error
	_, encErr := serializer.Encode(Command1{})
	if encErr == nil {
		t.Errorf("serializer expected to return an error for non proto generated messages but di not")
	}

	cmd := getProtoCommand()
	encodedBytes, encErr := serializer.Encode(cmd)
	if encErr != nil {
		t.Fatalf("encoding returned an error: %v", encErr)
	}

	//decoding an unregistered schema fails and returns an error
	if _, decErr := serializer.Decode(encodedBytes, "kong"); decErr == nil {
		t.Errorf("decoding an unregistred schema  is expected to return an error but did not")
	}

	serializer.Register(cmd)
	//decoding junk fails and returns an error
	junk := make([]byte, 16)
	rand.Read(junk)
	if _, decErr := serializer.Decode(junk, cmd.SchemaName()); decErr == nil {
		t.Errorf("decoding junk is expected to return an error but did not")
	}

}
