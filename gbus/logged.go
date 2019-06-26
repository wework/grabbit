// Copyright © 2019 Vladislav Shub <vladislav.shub@wework.com>
// All rights reserved to the We Company.

package gbus

import (
	"github.com/sirupsen/logrus"
)

var _ Logged = &Glogged{}

type Glogged struct {
	log logrus.FieldLogger
}

func (gl *Glogged) SetLogger(entry logrus.FieldLogger) {
	if gl == nil {
		gl = &Glogged{}
	}
	gl.log = entry
}

func (gl *Glogged) Log() logrus.FieldLogger {
	if gl == nil {
		gl = &Glogged{}
	}
	if gl.log == nil {
		gl.log = logrus.New()
	}
	return gl.log
}
