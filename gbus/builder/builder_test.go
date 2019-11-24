package builder

import "testing"

func TestAddParseTimeToConnStr(t *testing.T) {
	if AddParseTimeToConnStr("rhinof:rhinof@/rhinof") != "rhinof:rhinof@/rhinof?parseTime=true" {
		t.Errorf("Failed adding parseTime=true to connection string")
	}

	if AddParseTimeToConnStr("rhinof:rhinof@/rhinof?parseTime=false") != "rhinof:rhinof@/rhinof?parseTime=true" {
		t.Errorf("Failed setting parseTime=true to connection string")
	}

	if AddParseTimeToConnStr("rhinof:rhinof@/rhinof?otherParam=yes") != "rhinof:rhinof@/rhinof?otherParam=yes&parseTime=true" {
		t.Errorf("Failed adding parseTime=true to connection string with other parameters")
	}
}
