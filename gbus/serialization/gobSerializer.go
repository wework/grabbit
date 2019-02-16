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
	g := &GobSerializer{
		lock:              &sync.Mutex{},
		registeredSchemas: make(map[string]bool),
	}
	// g.Register(&gobMessage{})
	return g
}

//EncoderID implements MessageEncoding.EncoderID
func (gs *GobSerializer) EncoderID() string {
	return "gob"
}

//Encode implements MessageEncoding.Encode
func (gs *GobSerializer) Encode(message gbus.Message) ([]byte, error) {
	gs.Register(message)

	gmsg := &gobMessage{
		Payload: message,
	}

	var buf bytes.Buffer
	//TODO: Switch from gob to Avro (https://github.com/linkedin/goavro)
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(gmsg)
	return buf.Bytes(), err
}

//Decode implements MessageEncoding.Decode
func (*GobSerializer) Decode(data []byte) (gbus.Message, error) {
	reader := bytes.NewReader(data)
	dec := gob.NewDecoder(reader)
	var tm gobMessage
	decErr := dec.Decode(&tm)
	return tm.Payload, decErr
}

//Register implements MessageEncoding.Register
func (gs *GobSerializer) Register(obj gbus.Message) {
	gs.lock.Lock()
	defer gs.lock.Unlock()
	fqn := gbus.GetFqn(obj)
	if !gs.registeredSchemas[fqn] {
		log.Printf("registering schema to gob\n%v", fqn)
		gob.Register(obj)
		gs.registeredSchemas[fqn] = true
	}
}

type gobMessage struct {
	Payload gbus.Message
}
