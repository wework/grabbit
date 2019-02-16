package tests

type Command1 struct {
	Data string
}

func (Command1) FQN() string {
	return "grabbit.tests.command1"
}

type Command2 struct {
	Data string
}

func (Command2) FQN() string {
	return "grabbit.tests.command2"
}

type Reply1 struct {
	Data string
}

func (Reply1) FQN() string {
	return "grabbit.tests.reply1"
}

type Reply2 struct {
	Data string
}

func (Reply2) FQN() string {
	return "grabbit.tests.reply2"
}

type Event1 struct {
	Data string
}

func (Event1) FQN() string {
	return "grabbit.tests.event1"
}

type Event2 struct {
	Data string
}

func (Event2) FQN() string {
	return "grabbit.tests.event2"
}
