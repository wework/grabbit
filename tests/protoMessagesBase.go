package tests

//SchemaName implements the gbus.Message contract for ProtoCommand
func (*ProtoCommand) SchemaName() string {
	return "ProtoCommand"
}

func (*EmptyProtoCommand) SchemaName() string {
	return "EmptyProtoCommand"
}
