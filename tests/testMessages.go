package tests

import "github.com/rhinof/grabbit/gbus"

var _ gbus.Message = &Command1{}
var _ gbus.Message = &Command2{}
var _ gbus.Message = &Reply1{}
var _ gbus.Message = &Reply2{}
var _ gbus.Message = &Event1{}
var _ gbus.Message = &Event2{}

type Command1 struct {
	Data string
}

func (Command1) SchemaName() string {
	return "grabbit.tests.Command1"
}

type Command2 struct {
	Data string
}

func (Command2) SchemaName() string {
	return "grabbit.tests.Command2"
}

type Reply1 struct {
	Data string
}

func (Reply1) SchemaName() string {
	return "grabbit.tests.Reply1"
}

type Reply2 struct {
	Data string
}

func (Reply2) SchemaName() string {
	return "grabbit.tests.Reply2"
}

type Event1 struct {
	Data string
}

func (Event1) SchemaName() string {
	return "grabbit.tests.Event1"
}

type Event2 struct {
	Data string
}

func (Event2) SchemaName() string {
	return "grabbit.tests.Event2"
}
