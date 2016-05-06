package csgo

import "testing"

func createCases() []BasicDataStore {
	return []BasicDataStore{
		{INT, []int{1, 2, 3}},
		{FLOAT, []float64{3.1, 2.2, 1.3}},
		{STRING, []string{"test1", "arg2", "test3"}},
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
