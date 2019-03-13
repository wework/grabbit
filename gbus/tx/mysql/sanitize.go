package mysql

import "regexp"

func sanitizeTableName(dirty string) string {

	var re = regexp.MustCompile("-|;|\\|")
	sanitized := re.ReplaceAllString(dirty, "")
	return sanitized
}
