package saga

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wework/grabbit/gbus"
)

//TimeoutManager manages timeouts for sagas
//TODO:Make it persistent
type TimeoutManager struct {
	Bus         gbus.Bus
	Log         func() logrus.FieldLogger
	TimeoutSaga func(*sql.Tx, string) error
	Txp         gbus.TxProvider
}

//RequestTimeout requests a timeout from the timeout manager
func (tm *TimeoutManager) RequestTimeout(svcName, sagaID string, duration time.Duration) {

	go func(svcName, sagaID string, tm *TimeoutManager) {
		c := time.After(duration)
		<-c
		//TODO:if the bus is not transactional, moving forward we should not allow using sagas in a non transactional bus
		if tm.Txp == nil {
			tme := tm.TimeoutSaga(nil, sagaID)
			if tme != nil {
				tm.Log().WithError(tme).WithField("sagaID", sagaID).Error("timing out a saga failed")
			}
			return
		}
		tx, txe := tm.Txp.New()
		if txe != nil {
			tm.Log().WithError(txe).Warn("timeout manager failed to create a transaction")
		} else {
			callErr := tm.TimeoutSaga(tx, sagaID)
			if callErr != nil {
				tm.Log().WithError(callErr).WithField("sagaID", sagaID).Error("timing out a saga failed")
				rlbe := tx.Rollback()
				if rlbe != nil {
					tm.Log().WithError(rlbe).Warn("timeout manager failed to rollback transaction")
				}
			} else {
				cmte := tx.Commit()
				if cmte != nil {
					tm.Log().WithError(cmte).Warn("timeout manager failed to rollback transaction")
				}
			}
		}

	}(svcName, sagaID, tm)
}
