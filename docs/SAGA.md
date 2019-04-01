#Saga

grabbit makes it easier to implement orchistration based saga's by allowing to couple message exchange with a specific instance of persistant state.

##Defining a Saga

###Step 1 - Declare a struct that will hold the state of the saga
```go
import (
	"github.com/rhinof/grabbit/gbus"
)

type MySaga struct {
	Field1 string
	Field2 int
}

```
###Step 2 - Implement the gbus.Saga interface

In order to use your defined struct as a saga  you will need to implement the gbus.Saga interface inorder to let grabbit know the following things

	In response to what messages a new instance of the saga be created
	Which commands/replies and events should be routed to the saga instance
	How to create a new instance of the saga
	When the saga has completed (and should be removed)

In order to do all of the above you will need your saga to implement the gbus.Saga interface which is defined as following

```go
//Saga is the base interface for all Sagas.
type Saga interface {
	//StartedBy returns the messages that when received should create a new saga instance
	StartedBy() []Message
	/*
		RegisterAllHandlers passes in the HandlerRegister so that the saga can register
		the messages that it handles
	*/
	RegisterAllHandlers(register HandlerRegister)

	//IsComplete retruns if the saga is complete and can be discarded
	IsComplete() bool

	//New is a factory method used by the bus to crerate new instances of a saga
	New() Saga
}
```
###Step 3 - Register the saga with bus
```go

gb := getBus()
gb.RegisterSaga(MySaga{})

```
