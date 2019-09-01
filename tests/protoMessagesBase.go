package tests

//SchemaName implements the gbus.Message contract for ProtoCommand
func (*ProtoCommand) SchemaName() string {
	return "ProtoCommand"
}
