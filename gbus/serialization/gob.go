package serialization

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

var _ gbus.Serializer = &Gob{}

//Gob encodes and decodes messages using gob encoding
type Gob struct {
	lock              *sync.Mutex
	registeredSchemas map[string]reflect.Type
}

//NewGobSerializer create a new instance of  Gob
func NewGobSerializer() gbus.Serializer {
	g := &Gob{
		lock:              &sync.Mutex{},
		registeredSchemas: make(map[string]reflect.Type),
	}
	// g.Register(&gobMessage{})
	return g
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
func (gs *Gob) Decode(buffer []byte, schemaName string) (gbus.Message, error) {
	reader := bytes.NewReader(buffer)
	dec := gob.NewDecoder(reader)

	t, ok := gs.registeredSchemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("could not find the message type in gob registry, type: %s", schemaName)
	}
	msg := reflect.New(t).Interface()

	decErr := dec.Decode(msg)
	if decErr != nil {
		return nil, decErr
	}
	gmsg, ok := msg.(gbus.Message)
	if !ok {
		return nil, fmt.Errorf("could not cast %v to gbus.Message", msg)
	}
	return gmsg, nil
}

//Register implements Serializer.Register
func (gs *Gob) Register(obj gbus.Message) {
	gs.lock.Lock()
	defer gs.lock.Unlock()
	if gs.registeredSchemas[obj.SchemaName()] == nil {
		log.Printf("registering schema to gob\n%v", obj.SchemaName())
		gob.Register(obj)
		gs.registeredSchemas[obj.SchemaName()] = reflect.ValueOf(obj).Type()
	}
}
