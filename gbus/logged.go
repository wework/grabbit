package gbus

import (
	"github.com/sirupsen/logrus"
)

var _ Logged = &Glogged{}

//Glogged provides an easy way for structs with in the grabbit package to participate in the general logging schema of the bus
type Glogged struct {
	log logrus.FieldLogger
}

//SetLogger sets the default logrus.FieldLogger that should be used when logging a new message
func (gl *Glogged) SetLogger(entry logrus.FieldLogger) {
	if gl == nil {
		gl = &Glogged{}
	}
	gl.log = entry
}

//Log returns the set default log or a new instance of a logrus.FieldLogger
func (gl *Glogged) Log() logrus.FieldLogger {
	if gl == nil {
		gl = &Glogged{}
	}
	if gl.log == nil {
		gl.log = logrus.New()
	}
	return gl.log
}
