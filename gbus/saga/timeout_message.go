package saga

import (
	"github.com/wework/grabbit/gbus"
)

var _ gbus.Message = SagaTimeoutMessage{}

type SagaTimeoutMessage struct{}

func (SagaTimeoutMessage) SchemaName() string {
	return "grabbit.timeout"
}
