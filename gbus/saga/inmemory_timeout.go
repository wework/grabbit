package saga

import (
	"database/sql"
	"time"

	"github.com/wework/grabbit/gbus"
)

var _ gbus.TimeoutManager = &InMemoryTimeoutManager{}

//InMemoryTimeoutManager should not be used in production
type InMemoryTimeoutManager struct {
	glue *Glue
	txp  gbus.TxProvider
}

//RegisterTimeout requests a timeout from the timeout manager
func (tm *InMemoryTimeoutManager) RegisterTimeout(tx *sql.Tx, sagaID string, duration time.Duration) error {

	go func(svcName, sagaID string, tm *InMemoryTimeoutManager) {
		c := time.After(duration)
		<-c
		//TODO:if the bus is not transactional, moving forward we should not allow using sagas in a non transactional bus
		if tm.txp == nil {
			tme := tm.glue.TimeoutSaga(nil, sagaID)
			if tme != nil {
				tm.glue.log().WithError(tme).WithField("sagaID", sagaID).Error("timing out a saga failed")
			}
			return
		}
		tx, txe := tm.txp.New()
		if txe != nil {
			tm.glue.log().WithError(txe).Warn("timeout manager failed to create a transaction")
		} else {
			callErr := tm.glue.TimeoutSaga(tx, sagaID)
			if callErr != nil {
				tm.glue.log().WithError(callErr).WithField("sagaID", sagaID).Error("timing out a saga failed")
				rlbe := tx.Rollback()
				if rlbe != nil {
					tm.glue.log().WithError(rlbe).Warn("timeout manager failed to rollback transaction")
				}
			} else {
				cmte := tx.Commit()
				if cmte != nil {
					tm.glue.log().WithError(cmte).Warn("timeout manager failed to rollback transaction")
				}
			}
		}

	}(tm.glue.svcName, sagaID, tm)

	return nil
}

//ClearTimeout clears a timeout for a specific saga
func (tm *InMemoryTimeoutManager) ClearTimeout(tx *sql.Tx, sagaID string) error {
	return nil
}

//AcceptTimeoutFunction accepts the timeouting function
func (tm *InMemoryTimeoutManager) AcceptTimeoutFunction(fun func(tx *sql.Tx, sagaID string) error) {}
