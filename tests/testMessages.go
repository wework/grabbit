package tests

import "github.com/wework/grabbit/gbus"

var _ gbus.Message = &Command1{}
var _ gbus.Message = &Command2{}
var _ gbus.Message = &Reply1{}
var _ gbus.Message = &Reply2{}
var _ gbus.Message = &Event1{}
var _ gbus.Message = &Event2{}

//PoisonMessage is a malformed message to test posion pill scenarios
type PoisonMessage struct {
}

//SchemaName implementing gbus.Message
func (PoisonMessage) SchemaName() string {
	//an empty schema name will result in a message being treated as poison
	return ""
}

//Command1 for testing
type Command1 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Command1) SchemaName() string {
	return "grabbit.tests.Command1"
}

//Command2 for testing
type Command2 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Command2) SchemaName() string {
	return "grabbit.tests.Command2"
}

//Reply1 for testing
type Reply1 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Reply1) SchemaName() string {
	return "grabbit.tests.Reply1"
}

//Reply2 for testing
type Reply2 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Reply2) SchemaName() string {
	return "grabbit.tests.Reply2"
}

//Event1 for testing
type Event1 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Event1) SchemaName() string {
	return "grabbit.tests.Event1"
}

//Event2 for testing
type Event2 struct {
	Data string
}

//SchemaName implementing gbus.Message
func (Event2) SchemaName() string {
	return "grabbit.tests.Event2"
}
