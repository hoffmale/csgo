package csgo

import "testing"

func fillDataStore(ds DataStore, data ...interface{}) DataStore {
	for _, value := range data {
		ds.AddRow(ds.GetDataType(), value)
	}
	return ds
}

func createDictEncodedDataStoreCases() []DictEncodedDataStore {
	return []DictEncodedDataStore{
		DictEncodedDataStore{INT, map[int]interface{}{0: 215, 1: 9e+14}, fillDataStore(NewBasicDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		DictEncodedDataStore{FLOAT, map[int]interface{}{0: 215.0e+20, 1: -9000e+14}, fillDataStore(NewBasicDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		DictEncodedDataStore{STRING, map[int]interface{}{0: "Max-Planck-Ring, Ilmenau", 1: "Mazeh, Damascus, Syria"}, fillDataStore(NewBasicDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		DictEncodedDataStore{INT, map[int]interface{}{0: 215, 1: 9e+14}, fillDataStore(NewRLEDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		DictEncodedDataStore{FLOAT, map[int]interface{}{0: 215.0e+20, 1: -9000e+14}, fillDataStore(NewRLEDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		DictEncodedDataStore{STRING, map[int]interface{}{0: "Max-Planck-Ring, Ilmenau", 1: "Mazeh, Damascus, Syria"}, fillDataStore(NewRLEDataStore(INT), 1, 1, 1, 0, 0, 0, 1)},
		//{INT, {0: 215, 1: 9e+14}, {int(1), int(1), int(1), int(0), int(0), int(0), int(1)}},
		//{FLOAT, {0: 215.0e+20, 1: -9000e+14}, int(1), int(1), int(1), int(0), int(0), int(0), int(1)}},
		//{STRING, {0: "Max-Planck-Ring, Ilmenau", 1: "Mazeh, Damascus, Syria"}, int(1), int(1), int(1), int(0), int(0), int(0), int(1)}},
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
