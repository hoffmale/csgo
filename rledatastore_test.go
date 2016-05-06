package csgo

import "testing"

func createRLEDataStoreCases() []RLEDataStore {
	return []RLEDataStore{
		{INT, []RLEDataEntry{{1, int(1)}, {1, int(2)}, {1, int(3)}}},
		{FLOAT, []RLEDataEntry{{1, float64(3.1)}, {1, float64(2.2)}, {1, float64(1.3)}}},
		{STRING, []RLEDataEntry{{1, "test1"}, {1, "arg2"}, {1, "test3"}}},
		{INT, []RLEDataEntry{}},
	}
}

func TestRLEDataStoreAddRow(t *testing.T) {
	for _, ds := range createRLEDataStoreCases() {
		testDataStoreAddRow(&ds, t)
	}
}

func TestRLEDataStoreGetRow(t *testing.T) {
	for _, ds := range createRLEDataStoreCases() {
		testDataStoreGetRow(&ds, t)
	}
}

func TestRLEDataStoreGetNumRows(t *testing.T) {
	for _, ds := range createRLEDataStoreCases() {
		testDataStoreGetNumRows(&ds, t)
	}
}

func TestRLEDataStoreGetDataType(t *testing.T) {
	for _, ds := range createRLEDataStoreCases() {
		testDataStoreGetDataType(&ds, ds.DataType, t)
	}
}
