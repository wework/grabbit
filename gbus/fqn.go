package gbus

import "reflect"

//GetTypeFQN gets the "fully qualified name" of a type. meaning the package path + typename
func GetTypeFQN(t reflect.Type) string {

	if kind := t.Kind(); kind == reflect.Ptr {
		return t.Elem().PkgPath() + "." + t.Elem().Name()
	}
	return t.PkgPath() + "." + t.Name()
}

//GetFqn gets the "fully qualified name" of an interface. meaning the package path + typename
func GetFqn(obj interface{}) string {
	t := reflect.TypeOf(obj)
	return GetTypeFQN(t)
}
