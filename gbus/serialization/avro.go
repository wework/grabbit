package serialization

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"sync"

	kafka "github.com/dangkaka/go-kafka-avro"
	"github.com/linkedin/goavro"
	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
)

var _ gbus.Serializer = &Avro{}

type avroDeserializer func(r io.Reader) (interface{}, error)

type avroRelation struct {
	SchemaID     int
	Schema       string
	Codec        *goavro.Codec
	ObjType      reflect.Type
	Deserializer avroDeserializer
}

//AvroMessageGenerated an interface for the https://github.com/actgardner/gogen-avro since it doesn't have one :(
type AvroMessageGenerated interface {
	Schema() string
	SchemaName() string
	Serialize(w io.Writer) error
}

//Avro a serializer for GBus uses Avro
type Avro struct {
	lock                 *sync.Mutex
	registeredSchemas    map[string]*avroRelation
	registeredObjects    map[int]*avroRelation
	schemaRegistryUrls   []string
	schemaRegistryClient *kafka.CachedSchemaRegistryClient
}

//NewMessageEncoding creates an instance of Avro and returns gbus.Serializer
func NewMessageEncoding(schemaRegistryUrls ...string) gbus.Serializer {
	return NewAvroSerializer(schemaRegistryUrls...)
}

//NewAvroSerializer creates a new instance of Avro and returns it
func NewAvroSerializer(schemaRegistryUrls ...string) *Avro {
	return &Avro{
		schemaRegistryUrls:   schemaRegistryUrls,
		schemaRegistryClient: kafka.NewCachedSchemaRegistryClient(schemaRegistryUrls),
		registeredSchemas:    make(map[string]*avroRelation),
		registeredObjects:    make(map[int]*avroRelation),
		lock:                 &sync.Mutex{},
	}
}

//Name implements Serializer.Name
func (as *Avro) Name() string {
	return "avro/binary"
}

//Encode encodes an object into a byte array
func (as *Avro) Encode(obj gbus.Message) (msg []byte, err error) {

	name := obj.SchemaName()
	rel, ok := as.registeredSchemas[name]
	if !ok {
		err := fmt.Errorf("not a registered object :(")
		logrus.WithError(err).WithField("name", name).Error("not a registered type")
		return nil, err
	}

	binarySchemaID := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaID, uint32(rel.SchemaID))
	msg = make([]byte, 0)
	// first byte is magic byte, always 0 for now
	msg = append(msg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	msg = append(msg, binarySchemaID...)

	tobj, ok := obj.(AvroMessageGenerated)
	if !ok {
		err := fmt.Errorf("could not convert obj to AvroMessageGenerated")
		logrus.WithError(err).Error("could not convert object")
		return nil, err
	}

	var buf bytes.Buffer
	bufWriter := bufio.NewWriter(&buf)
	err = tobj.Serialize(bufWriter)
	if err != nil {
		logrus.
			WithError(err).
			WithField("name", name).
			Error("could not convert obj to bindata")
		return nil, err
	}
	err = bufWriter.Flush()
	if err != nil {
		logrus.WithError(err).Error("could not flush to buffer")
		return nil, err
	}
	//avro serialized data in Avro’s binary encoding
	binaryValue := buf.Bytes()
	msg = append(msg, binaryValue...)
	return msg, nil
}

//Decode decodes a byte array into an object
func (as *Avro) Decode(buffer []byte, schemaName string) (msg gbus.Message, err error) {
	schemaID := binary.BigEndian.Uint32(buffer[1:5])
	rel, ok := as.registeredObjects[int(schemaID)]
	if !ok {
		err = fmt.Errorf("could not find avroRelation")
		logrus.WithError(err).Error("no avroRelation for obj in registeredObjects")
		return
	}
	var buf bytes.Buffer
	buf.Write(buffer[5:])

	o, err := rel.Deserializer(&buf)
	if err != nil {
		return
	}
	obj, ok := o.(gbus.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to gbus.Message", o)
	}
	return obj, nil
	//// Convert binary Avro data back to native Go form
	//avroObj, _, err := rel.Codec.NativeFromBinary(buffer[5:])
	//if err != nil {
	//	logrus.WithError(err).Error("could not convert binary to native")
	//	return
	//}
	//
	//obj = reflect.New(rel.ObjType)
	//err = mapstructure.Decode(avroObj, obj)
	//if err != nil {
	//	logrus.WithError(err).
	//		WithField("avro_obj", avroObj).
	//		WithField("type", rel.ObjType).
	//		Error("parse message into object")
	//}
	//return
}

//Register not really used here :(
func (as *Avro) Register(obj gbus.Message) {
	// TODO: we should think what is the best way to do this
}

//RegisterAvroMessageFromFile reads an avro schema (.avsc) and registers it to a topic and binds it to an object (obj)
func (as *Avro) RegisterAvroMessageFromFile(schemaName, schemaPath, namespace string, obj AvroMessageGenerated, deserializer avroDeserializer) (err error) {
	dat, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		logrus.WithError(err).WithField("schema_path", schemaPath).Error("could not find schema")
		return
	}
	return as.RegisterAvroMessage(schemaName, namespace, string(dat), obj, deserializer)
}

//RegisterAvroMessage registers a schema to a topic and binds it to an object (obj)
func (as *Avro) RegisterAvroMessage(schemaName, namespace, schema string, obj gbus.Message, deserializer avroDeserializer) (err error) {
	as.lock.Lock()
	defer as.lock.Unlock()
	if _, ok := as.registeredSchemas[obj.SchemaName()]; !ok {
		logrus.WithField("SchemaName", obj.SchemaName()).Debug("registering schema to avro")
		rel := &avroRelation{
			Schema:       schema,
			Deserializer: deserializer,
		}
		rel.Codec, err = goavro.NewCodec(schema)
		if err != nil {
			logrus.WithError(err).Error("could not get codec for schema")
			return
		}
		rel.SchemaID, err = as.registerOrGetSchemaID(obj.SchemaName(), rel.Codec)
		if err != nil {
			logrus.WithError(err).Error("could not get schema id")
			return
		}
		rel.ObjType = reflect.TypeOf(obj)
		as.registeredSchemas[obj.SchemaName()] = rel
		as.registeredObjects[rel.SchemaID] = rel
	}
	return
}

//registerOrGetSchemaID get schema id from schema-registry service
func (as *Avro) registerOrGetSchemaID(topic string, avroCodec *goavro.Codec) (schemaID int, err error) {
	schemaID = 0
	schemaID, err = as.schemaRegistryClient.IsSchemaRegistered(topic, avroCodec)
	if err != nil {
		schemaID, err = as.schemaRegistryClient.CreateSubject(topic, avroCodec)
		if err != nil {
			return 0, err
		}
	}
	return
}
