package saga

import (
	"database/sql"
	"errors"
)

//InMemorySagaStore stores the saga instances in-memory, not intended for production use
type InMemorySagaStore struct {
	instances map[*Def][]*Instance
}

//GetSagaByID implements SagaStore.GetSagaByID
func (store *InMemorySagaStore) GetSagaByID(tx *sql.Tx, sagaID string) (*Instance, error) {
	for _, instances := range store.instances {
		for _, instance := range instances {
			if instance.ID == sagaID {
				return instance, nil
			}
		}
	}
	return nil, errors.New("no saga found for provided id")
}

//SaveNewSaga implements SagaStore.SaveNewSaga
func (store *InMemorySagaStore) SaveNewSaga(tx *sql.Tx, def *Def, newInstance *Instance) error {
	instances := store.instances[def]
	if instances == nil {
		instances = make([]*Instance, 0)

	}
	instances = append(instances, newInstance)
	store.instances[def] = instances

	return nil

}

//UpdateSaga implements SagaStore.UpdateSaga
func (store *InMemorySagaStore) UpdateSaga(tx *sql.Tx, instance *Instance) error {

	return nil
}

//DeleteSaga implements SagaStore.DeleteSaga
func (store *InMemorySagaStore) DeleteSaga(tx *sql.Tx, instance *Instance) error {

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

//NewInMemoryStore is a factory method for the InMemorySagaStore
func NewInMemoryStore() Store {
	return &InMemorySagaStore{
		instances: make(map[*Def][]*Instance)}
}
