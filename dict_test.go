package csgo

import "testing"

func createDictEncodedDataStoreCases() []DictEncodedDataStore {
	return []DictEncodedDataStore{
        {	true,
			INT,
			{0: int(215), 1: int(9e+14)},
			{1, 1, 1, 0, 0, 0, 1},
		},
        {	false,
			FLOAT,
			{0: 215.0e+20, 1: -9000e+14},
			{int(1), int(1), int(1), int(0), int(0), int(0), int(1)},
		},
	    {	true,
			STRING,
			{0: "Max-Planck-Ring, Ilmenau", 1: "Mazeh, Damascus, Syria"},
			{int(1), int(1), int(1), int(0), int(0), int(0), int(1)},
		},
	}
}

func TestDictEncodedDataStoreAddRow(t *testing.T) {
	for _, ds := range createDictEncodedDataStoreCases() {
		testDataStoreAddRow(&ds, t)
	}
}

func TestDictEncodedDataStoreGetRow(t *testing.T) {
	for _, ds := range createDictEncodedDataStoreCases() {
		testDataStoreGetRow(&ds, t)
	}
}

func TestDictEncodedDataStoreGetNumRows(t *testing.T) {
	for _, ds := range createDictEncodedDataStoreCases() {
		testDataStoreGetNumRows(&ds, t)
	}
}

func TestDictEncodedDataStoreGetDataType(t *testing.T) {
	for _, ds := range createDictEncodedDataStoreCases() {
		testDataStoreGetDataType(&ds, ds.DataType, t)
	}
}
