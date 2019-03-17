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

	"github.com/linkedin/goavro"
	"github.com/sirupsen/logrus"

	"github.com/dangkaka/go-kafka-avro"
	"github.com/rhinof/grabbit/gbus"
)

var _ gbus.MessageEncoding = &AvroSerializer{}

type avroDeserializer func(r io.Reader) (interface{}, error)

type avroRelation struct {
	SchemaId     int
	Schema       string
	SchemaName   string
	Codec        *goavro.Codec
	ObjType      reflect.Type
	Deserializer avroDeserializer
}

type AvroMessageGenerated interface {
	Schema() string
	SchemaName() string
	Serialize(w io.Writer) error
}

//AvroSerializer a serializer for GBus uses Avro
type AvroSerializer struct {
	lock                 *sync.Mutex
	registeredSchemas    map[string]*avroRelation
	registeredObjects    map[int]*avroRelation
	schemaRegistryUrls   []string
	schemaRegistryClient *kafka.CachedSchemaRegistryClient
}

//NewMessageEncoding creates an instance of AvroSerializer and returns gbus.MessageEncoding
func NewMessageEncoding(schemaRegistryUrls ...string) gbus.MessageEncoding {
	return NewAvroSerializer(schemaRegistryUrls...)
}

//NewAvroSerializer creates a new instance of AvroSerializer and returns it
func NewAvroSerializer(schemaRegistryUrls ...string) *AvroSerializer {
	return &AvroSerializer{
		schemaRegistryUrls:   schemaRegistryUrls,
		schemaRegistryClient: kafka.NewCachedSchemaRegistryClient(schemaRegistryUrls),
		registeredSchemas:    make(map[string]*avroRelation),
		registeredObjects:    make(map[int]*avroRelation),
		lock:                 &sync.Mutex{},
	}
}

//EncoderID implements MessageEncoding.EncoderID
func (as *AvroSerializer) EncoderID() string {
	return "avro"
}

//Encode encodes an object into a byte array
func (as *AvroSerializer) Encode(obj gbus.Message) (msg []byte, err error) {

	name := obj.SchemaName()
	rel, ok := as.registeredSchemas[name]
	if !ok {
		err = fmt.Errorf("not a registered obbject :(")
		logrus.WithError(err).WithField("name", name).Error("not a registered type")
		return
	}

	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(rel.SchemaId))
	msg = make([]byte, 0)
	// first byte is magic byte, always 0 for now
	msg = append(msg, byte(0))
	//4-byte schema ID as returned by the Schema Registry
	msg = append(msg, binarySchemaId...)

	tobj, ok := obj.(AvroMessageGenerated)
	if !ok {
		err = fmt.Errorf("could not convert obj to AvroMessageGenerated")
		logrus.WithError(err).WithField("obj", obj).Error("could not convert object")
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

	return
}

//Decode decodes a byte array into an object
func (as *AvroSerializer) Decode(buffer []byte) (obj gbus.Message, err error) {
	schemaId := binary.BigEndian.Uint32(buffer[1:5])
	rel, ok := as.registeredObjects[int(schemaId)]
	if !ok {
		err = fmt.Errorf("could not find avroRelation")
		logrus.WithError(err).Error("no avroRelation for obj in registeredObjects")
		return
	}
	var buf bytes.Buffer
	buf.Write(buffer[5:])

	o, e := rel.Deserializer(&buf)
	return o.(gbus.Message), e
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
func (as *AvroSerializer) Register(obj gbus.Message) {
	// TODO: we should think what is the best way to do this
}

//RegisterAvroMessageFromFile reads an avro schema (.avsc) and registers it to a topic and binds it to an object (obj)
func (as *AvroSerializer) RegisterAvroMessageFromFile(schemaName, schemaPath, namespace string, obj AvroMessageGenerated, deserializer avroDeserializer) (err error) {
	dat, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		logrus.WithError(err).WithField("schema_path", schemaPath).Error("could not find schema")
		return
	}
	return as.RegisterAvroMessage(schemaName, namespace, string(dat), obj, deserializer)
}

//RegisterAvroMessage registers a schema to a topic and binds it to an object (obj)
func (as *AvroSerializer) RegisterAvroMessage(schemaName, namespace, schema string, obj AvroMessageGenerated, deserializer avroDeserializer) (err error) {
	as.lock.Lock()
	defer as.lock.Unlock()
	if _, ok := as.registeredSchemas[obj.SchemaName()]; !ok {
		rel := &avroRelation{
			SchemaName:   schemaName,
			Schema:       schema,
			Deserializer: deserializer,
		}
		rel.Codec, err = goavro.NewCodec(schema)
		if err != nil {
			logrus.WithError(err).Error("could not get codec for schema")
			return
		}
		rel.SchemaId, err = as.registerOrGetSchemaId(fmt.Sprintf("%s.%s", namespace, schemaName), rel.Codec)
		if err != nil {
			logrus.WithError(err).Error("could not get schema id")
			return
		}
		rel.ObjType = reflect.TypeOf(obj)
		as.registeredSchemas[obj.SchemaName()] = rel
		as.registeredObjects[rel.SchemaId] = rel
	}
	return
}

//getSchema get schema id from schema-registry service
func (as *AvroSerializer) getSchema(id int) (*goavro.Codec, error) {
	codec, err := as.schemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

//registerOrGetSchemaId get schema id from schema-registry service
func (as *AvroSerializer) registerOrGetSchemaId(topic string, avroCodec *goavro.Codec) (schemaId int, err error) {
	schemaId = 0
	schemaId, err = as.schemaRegistryClient.IsSchemaRegistered(topic, avroCodec)
	if err != nil {
		schemaId, err = as.schemaRegistryClient.CreateSubject(topic, avroCodec)
		if err != nil {
			return 0, err
		}
	}
	return
}
