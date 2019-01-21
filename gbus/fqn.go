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

//GetFqns returns the "fully qualified name" of every  interface in the input slice
func GetFqns(objs []interface{}) []string {
	fqns := make([]string, 0)
	for _, obj := range objs {
		fqn := GetFqn(obj)
		fqns = append(fqns, fqn)
	}
	return fqns
}
