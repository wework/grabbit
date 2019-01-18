package gbus

import "reflect"

func GetTypeFQN(t reflect.Type) string {

	if kind := t.Kind(); kind == reflect.Ptr {
		return t.Elem().PkgPath() + "." + t.Elem().Name()
	}
	return t.PkgPath() + "." + t.Name()
}

func GetFqn(obj interface{}) string {
	t := reflect.TypeOf(obj)
	return GetTypeFQN(t)
}

func GetFqns(objs []interface{}) []string {
	fqns := make([]string, 0)
	for _, obj := range objs {
		fqn := GetFqn(obj)
		fqns = append(fqns, fqn)
	}
	return fqns
}
