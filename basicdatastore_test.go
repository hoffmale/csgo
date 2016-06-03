package csgo

import "testing"

func createCases() []BasicDataStore {
	return []BasicDataStore{
		{INT, 0, []interface{}{int(1), int(2), int(3)}},
		{FLOAT, 0, []interface{}{float64(3.1), float64(2.2), float64(1.3)}},
		{STRING, 0, []interface{}{"test1", "arg2", "test3"}},
	}
}

func TestBasicDataStoreAddRow(t *testing.T) {
	for _, ds := range createCases() {
		testDataStoreAddRow(&ds, t)
	}
}

func TestBasicDataStoreGetRow(t *testing.T) {
	for _, ds := range createCases() {
		testDataStoreGetRow(&ds, t)
	}
}

func TestBasicDataStoreGetNumRows(t *testing.T) {
	for _, ds := range createCases() {
		testDataStoreGetNumRows(&ds, t)
	}
}

func TestBasicDataStoreGetDataType(t *testing.T) {
	for _, ds := range createCases() {
		testDataStoreGetDataType(&ds, ds.DataType, t)
	}
}
