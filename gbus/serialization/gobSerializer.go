package serialization

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	"github.com/rhinof/grabbit/gbus"
)

//GobSerializer encodes and decodes messages using gob encoding
type GobSerializer struct {
	lock              *sync.Mutex
	registeredSchemas map[string]bool
}

//NewGobSerializer create a new instance of  GobSerializer
func NewGobSerializer() gbus.MessageEncoding {
	return &GobSerializer{
		lock:              &sync.Mutex{},
		registeredSchemas: make(map[string]bool)}
}

//Encode implements MessageEncoding.Encode
func (*GobSerializer) Encode(message *gbus.BusMessage) ([]byte, error) {
	gob.Register(message.Payload)

	var buf bytes.Buffer
	//TODO: Switch from gob to Avro (https://github.com/linkedin/goavro)
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(message)
	return buf.Bytes(), err
}

//Decode implements MessageEncoding.Decode
func (*GobSerializer) Decode(data []byte) (*gbus.BusMessage, error) {
	reader := bytes.NewReader(data)
	dec := gob.NewDecoder(reader)
	var tm gbus.BusMessage
	decErr := dec.Decode(&tm)
	return &tm, decErr
}

//Register implements MessageEncoding.Register
func (gs *GobSerializer) Register(obj interface{}) {
	gs.lock.Lock()
	defer gs.lock.Unlock()
	fqn := gbus.GetFqn(obj)
	if !gs.registeredSchemas[fqn] {
		log.Printf("registering schema to gob\n%v", fqn)
		gob.Register(obj)
		gs.registeredSchemas[fqn] = true
	}
}
