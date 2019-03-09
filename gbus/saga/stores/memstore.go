package stores

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/rhinof/grabbit/gbus"
	"github.com/rhinof/grabbit/gbus/saga"
)

//InMemorySagaStore stores the saga instances in-memory, not intended for production use
type InMemorySagaStore struct {
	instances map[reflect.Type][]*saga.Instance
}

//GetSagaByID implements SagaStore.GetSagaByID
func (store *InMemorySagaStore) GetSagaByID(tx *sql.Tx, sagaID string) (*saga.Instance, error) {
	for _, instances := range store.instances {
		for _, instance := range instances {
			if instance.ID == sagaID {
				return instance, nil
			}
		}
	}
	return nil, errors.New("no saga found for provided id")
}

//RegisterSagaType implements SagaStore.RegisterSagaType
func (store *InMemorySagaStore) RegisterSagaType(saga gbus.Saga) {

}

//SaveNewSaga implements SagaStore.SaveNewSaga
func (store *InMemorySagaStore) SaveNewSaga(tx *sql.Tx, sagaType reflect.Type, newInstance *saga.Instance) error {
	instances := store.instances[sagaType]
	if instances == nil {
		instances = make([]*saga.Instance, 0)

	}
	instances = append(instances, newInstance)
	store.instances[sagaType] = instances

	return nil

}

//UpdateSaga implements SagaStore.UpdateSaga
func (store *InMemorySagaStore) UpdateSaga(tx *sql.Tx, instance *saga.Instance) error {

	return nil
}

//DeleteSaga implements SagaStore.DeleteSaga
func (store *InMemorySagaStore) DeleteSaga(tx *sql.Tx, instance *saga.Instance) error {

	for key, value := range store.instances {
		var sagaIndexFound bool
		var sagaIndexToDelete int
		for i := 0; i < len(value); i++ {
			candidate := value[i]
			if candidate.ID == instance.ID {
				sagaIndexToDelete = i
				sagaIndexFound = true
				break
			}
		}
		if sagaIndexFound {
			value[sagaIndexToDelete] = value[len(value)-1]
			value = value[:len(value)-1]
			store.instances[key] = value

		}
	}
	return nil
}

//GetSagasByType implements SagaStore.GetSagasByType
func (store *InMemorySagaStore) GetSagasByType(tx *sql.Tx, t reflect.Type) ([]*saga.Instance, error) {
	instances := make([]*saga.Instance, 0)

	for key, val := range store.instances {
		if key == t {
			instances = append(instances, val...)
		}
	}

	return instances, nil
}

func (store *InMemorySagaStore) Purge() error {
	return nil
}

//NewInMemoryStore is a factory method for the InMemorySagaStore
func NewInMemoryStore() saga.Store {
	return &InMemorySagaStore{
		instances: make(map[reflect.Type][]*saga.Instance)}
}
