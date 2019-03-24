package serialization

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"sync"

	"github.com/rhinof/grabbit/gbus"
	"github.com/sirupsen/logrus"
)

var _ gbus.Serializer = &Gob{}

//Gob encodes and decodes messages using gob encoding
type Gob struct {
	lock              *sync.Mutex
	registeredSchemas map[string]reflect.Type
}

//NewGobSerializer create a new instance of  Gob
func NewGobSerializer() gbus.Serializer {
	return &Gob{
		lock:              &sync.Mutex{},
		registeredSchemas: make(map[string]reflect.Type),
	}
}

//Name implements Serializer.Name
func (gs *Gob) Name() string {
	return "gob"
}

//Encode implements Serializer.Encode
func (gs *Gob) Encode(message gbus.Message) ([]byte, error) {
	gs.Register(message)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(message)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//Decode implements Serializer.Decode
func (gs *Gob) Decode(buffer []byte, schemaName string) (msg gbus.Message, err error) {
	reader := bytes.NewReader(buffer)
	dec := gob.NewDecoder(reader)

	t, ok := gs.registeredSchemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("could not find the message type in gob registry, type: %s", schemaName)
	}
	tmsg := reflect.New(t).Interface()

	err = dec.Decode(tmsg)
	if err != nil {
		return nil, err
	}
	msg, ok = tmsg.(gbus.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to gbus.Message", tmsg)
	}
	return msg, nil
}

//Register implements Serializer.Register
func (gs *Gob) Register(obj gbus.Message) {
	gs.lock.Lock()
	defer gs.lock.Unlock()
	if gs.registeredSchemas[obj.SchemaName()] == nil {
		logrus.WithField("SchemaName", obj.SchemaName()).Debug("registering schema to gob")
		gob.Register(obj)
		gs.registeredSchemas[obj.SchemaName()] = reflect.ValueOf(obj).Type()
	}
}
