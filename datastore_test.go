package csgo

import (
	"reflect"
	"testing"
)

const testINT = int(5)
const testFLOAT = float64(3.1)
const testSTRING = "test"

func testDataStoreAddRow(ds DataStore, t *testing.T) {
	numRows := ds.GetNumRows()

	checkSuccess := func(dataType DataTypes, value interface{}) {
		index, err := ds.AddRow(dataType, value)

		if err != nil {
			t.Errorf("unexpected error: %#v", err)
			t.Fail()
			return
		}

		if index < 0 || index >= ds.GetNumRows() {
			t.Errorf("index out of bounds: %d is not element of [0, %d)", index, ds.GetNumRows())
			t.Fail()
		}
	}

	checkFailure := func(dataType DataTypes, value interface{}) {
		_, err := ds.AddRow(dataType, value)

		if err == nil {
			t.Errorf("unexpected success when adding %#v (DataType: %#v)", value, dataType)
			t.Fail()
		}
	}

	switch ds.GetDataType() {
	case INT:
		checkSuccess(INT, testINT)
		checkFailure(FLOAT, testFLOAT)
		checkFailure(STRING, testSTRING)
	case FLOAT:
		checkSuccess(FLOAT, testFLOAT)
		checkFailure(INT, testINT)
		checkFailure(STRING, testSTRING)
	case STRING:
		checkSuccess(STRING, testSTRING)
		checkFailure(INT, testINT)
		checkFailure(FLOAT, testFLOAT)
	}

	if ds.GetNumRows() != numRows+1 {
		t.Errorf("number of rows doesn't match expectations: %d (expected: %d)", ds.GetNumRows(), numRows+1)
		t.Fail()
	}
}

func testDataStoreGetRow(ds DataStore, t *testing.T) {
	checkAdd := func(dataType DataTypes, value interface{}) (int, error) {
		index, err := ds.AddRow(dataType, value)

		if err != nil {
			t.Errorf("unexpected error: %#v", err)
			t.Fail()
		}

		if index < 0 || index >= ds.GetNumRows() {
			t.Errorf("index out of bounds: %d is not element of [0, %d)", index, ds.GetNumRows())
			t.Fail()
		}

		return index, err
	}

	checkGetSuccess := func(rowIndex int) (interface{}, error) {
		value, err := ds.GetRow(rowIndex)

		if err != nil {
			t.Errorf("unexpected error: %#v", err)
			t.Fail()
		}

		return value, err
	}

	var index int
	var err error
	var expectValue interface{}

	switch ds.GetDataType() {
	case INT:
		index, err = checkAdd(INT, testINT)
		expectValue = testINT
	case FLOAT:
		index, err = checkAdd(FLOAT, testFLOAT)
		expectValue = testFLOAT
	case STRING:
		index, err = checkAdd(STRING, testSTRING)
		expectValue = testSTRING
	}

	if err == nil {
		value, err := checkGetSuccess(index)
		if err != nil {
			t.Errorf("unexpected error: %#v", err)
			t.Fail()
		} else {
			if !reflect.DeepEqual(value, expectValue) {
				t.Errorf("error: value (%#v) does not match expectations (%#v)", value, expectValue)
				t.Fail()
			}
		}
	}
}

func testDataStoreGetNumRows(ds DataStore, t *testing.T) {
	numRows := ds.GetNumRows()

	if numRows < 0 {
		t.Error("a datastore must not have less than 0 entries")
		t.Fail()
	}

	_, err := ds.GetRow(numRows)
	if err == nil {
		t.Error("either GetNumRows() does not return the number of entries or GetRow(GetNumRows()) returns an invalid value")
		t.Fail()
	}

	if numRows > 0 {
		_, err = ds.GetRow(numRows - 1)
		if err != nil {
			t.Error("the datastore is unable to return the value of the last row")
			t.Fail()
		}
	}
}

func testDataStoreGetDataType(ds DataStore, dataType DataTypes, t *testing.T) {
	if ds.GetDataType() != dataType {
		t.Errorf("expected to get type %#v, got %#v instead", dataType, ds.GetDataType())
		t.Fail()
	}
}
