package tx

import (
	"fmt"
	"regexp"
	"strings"
)

//SanitizeTableName returns a sanitizes and lower cased string for creating a table
func SanitizeTableName(dirty string) string {

	var re = regexp.MustCompile(`-|;|\\|`)
	sanitized := re.ReplaceAllString(dirty, "")
	return strings.ToLower(sanitized)
}

//GrabbitTableNameTemplate returns the tamplated grabbit table name for the  table type and service
func GrabbitTableNameTemplate(svcName, table string) string {
	sanitized := SanitizeTableName(svcName)
	templated := fmt.Sprintf("grabbit_%s_%s", sanitized, table)
	return strings.ToLower(templated)

}
