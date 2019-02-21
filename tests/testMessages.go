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
	return "grabbit.tests.command1"
}

type Command2 struct {
	Data string
}

func (Command2) SchemaName() string {
	return "grabbit.tests.command2"
}

type Reply1 struct {
	Data string
}

func (Reply1) SchemaName() string {
	return "grabbit.tests.reply1"
}

type Reply2 struct {
	Data string
}

func (Reply2) SchemaName() string {
	return "grabbit.tests.reply2"
}

type Event1 struct {
	Data string
}

func (Event1) SchemaName() string {
	return "grabbit.tests.event1"
}

type Event2 struct {
	Data string
}

func (Event2) SchemaName() string {
	return "grabbit.tests.event2"
}
