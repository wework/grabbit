package saga

import (
	"time"

	"github.com/wework/grabbit/gbus"
)

//TimeoutManager manages timeouts for sagas
//TODO:Make it persistent
type TimeoutManager struct {
	bus  gbus.Bus
	glue *Glue
	txp  gbus.TxProvider
}

//RequestTimeout requests a timeout from the timeout manager
func (tm *TimeoutManager) RequestTimeout(svcName, sagaID string, duration time.Duration) {

	go func(svcName, sagaID string, tm *TimeoutManager) {
		c := time.After(duration)
		<-c
		//TODO:if the bus is not transactional, moving forward we should not allow using sagas in a non transactional bus
		if tm.txp == nil {
			tme := tm.glue.timeoutSaga(nil, sagaID)
			if tme != nil {
				tm.glue.log().WithError(tme).WithField("sagaID", sagaID).Error("timing out a saga failed")
			}
			return
		}
		tx, txe := tm.txp.New()
		if txe != nil {
			tm.glue.log().WithError(txe).Warn("timeout manager failed to create a transaction")
		} else {
			callErr := tm.glue.timeoutSaga(tx, sagaID)
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

	}(svcName, sagaID, tm)
}
