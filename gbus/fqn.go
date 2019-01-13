package gbus

import "reflect"

func GetFqn(obj interface{}) string {
	t := reflect.TypeOf(obj)
	fqn := t.PkgPath() + "." + t.Name()
	return fqn
}

func GetFqns(objs []interface{}) []string {
	fqns := make([]string, 0)
	for _, obj := range objs {
		fqn := GetFqn(obj)
		fqns = append(fqns, fqn)
	}
	return fqns
}
