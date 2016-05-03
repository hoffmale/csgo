package csgo

import (
	"fmt"
	"reflect"
	"testing"
)

func TestNewColumn(t *testing.T) {
	cases := []struct {
		sig AttrInfo
	}{
		{sig: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}},
		{sig: AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}},
		{sig: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}},
		{sig: AttrInfo{}},
	}

	for _, testCase := range cases {
		col := NewColumn(testCase.sig)

		if col.Signature != testCase.sig {
			t.Fail()
			t.Error("signature mismatch")
		}
	}
}

func TestColumnAddRow(t *testing.T) {
	cases := []struct {
		sig           AttrInfo
		data          interface{} // preset values
		value         interface{}
		valueType     DataTypes
		shouldFail    bool
		expectedIndex int
		expectedData  interface{}
	}{
		// easiest cases
		{sig: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, data: []int{}, value: int(0), valueType: INT, shouldFail: false, expectedIndex: 0, expectedData: []int{0}},
		{sig: AttrInfo{Name: "testCol2", Type: FLOAT, Enc: NOCOMP}, data: []float64{}, value: float64(4.5), valueType: FLOAT, shouldFail: false, expectedIndex: 0, expectedData: []float64{4.5}},
		{sig: AttrInfo{Name: "testCol3", Type: STRING, Enc: NOCOMP}, data: []string{}, value: "testVal", valueType: STRING, shouldFail: false, expectedIndex: 0, expectedData: []string{"testVal"}},
		// type mismatch
		{sig: AttrInfo{Name: "testCol4", Type: INT, Enc: NOCOMP}, data: []int{}, value: float64(4.5), valueType: FLOAT, shouldFail: true, expectedIndex: -1, expectedData: []int{}},
		{sig: AttrInfo{Name: "testCol5", Type: INT, Enc: NOCOMP}, data: []int{}, value: "testVal", valueType: STRING, shouldFail: true, expectedIndex: -1, expectedData: []int{}},
		{sig: AttrInfo{Name: "testCol6", Type: FLOAT, Enc: NOCOMP}, data: []float64{}, value: int(3), valueType: INT, shouldFail: true, expectedIndex: -1, expectedData: []float64{}},
		{sig: AttrInfo{Name: "testCol7", Type: FLOAT, Enc: NOCOMP}, data: []float64{}, value: "testVal", valueType: STRING, shouldFail: true, expectedIndex: -1, expectedData: []float64{}},
		{sig: AttrInfo{Name: "testCol8", Type: STRING, Enc: NOCOMP}, data: []string{}, value: int(5), valueType: INT, shouldFail: true, expectedIndex: -1, expectedData: []string{}},
		{sig: AttrInfo{Name: "testCol9", Type: STRING, Enc: NOCOMP}, data: []string{}, value: float64(4.5), valueType: FLOAT, shouldFail: true, expectedIndex: -1, expectedData: []string{}},
		// check index
		{sig: AttrInfo{Name: "testCol10", Type: INT, Enc: NOCOMP}, data: []int{1, 2, 3, 4, 5, 6}, value: int(7), valueType: INT, shouldFail: false, expectedIndex: 6, expectedData: []int{1, 2, 3, 4, 5, 6, 7}},
	}

	for testcaseID, testcase := range cases {
		col := NewColumn(testcase.sig)
		col.Data = testcase.data

		index, err := col.AddRow(testcase.valueType, testcase.value)
		fmt.Println(col)
		if !reflect.DeepEqual(col.Data, testcase.expectedData) {
			t.Fail()
			t.Errorf("testcase %d: data mismatch (got %v, expected %v)", testcaseID, col.Data, testcase.expectedData)
		}

		if err != nil {
			if testcase.shouldFail {
				continue
			} else {
				t.Fail()
				t.Errorf("testcase %d unexpectedly failed: %v", testcaseID, err)
			}
		} else {
			if testcase.shouldFail {
				t.Fail()
				t.Errorf("testcase %d unexpectedly succeeded", testcaseID)
			} else {
				if index != testcase.expectedIndex {
					t.Fail()
					t.Errorf("testcase %d: expected index %d, got %d", testcaseID, testcase.expectedIndex, index)
				}
			}
		}
	}
}

func TestColumnImportRow(t *testing.T) {
	cases := []struct {
		sig           AttrInfo
		field         string
		shouldFail    bool
		expectedValue interface{}
	}{
		{sig: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, field: "321", shouldFail: false, expectedValue: int(321)},
	}

	for testcaseID, testcase := range cases {
		col := NewColumn(testcase.sig)

		index, err := col.ImportRow(testcase.field)
		fmt.Printf("testcase %d: index = %d\n", testcaseID, index)
		fmt.Println(col)
		if testcase.shouldFail {
			if err != nil {
				continue
			} else {
				t.Fail()
				t.Errorf("testcase %d unexpectedly did not fail", testcaseID)
			}
		} else {
			if err != nil {
				t.Fail()
				t.Errorf("testcase %d unexpectedly failed: %v", testcaseID, err)
				continue
			}

			value, err := col.GetRow(index)
			if err != nil {
				t.Fail()
				t.Errorf("testcase %d unexpectedly failed: %v", testcaseID, err)
				continue
			}

			if value != testcase.expectedValue {
				t.Fail()
				t.Errorf("testcase %d failed: value '%v' did not match the expected value '%v'", testcaseID, value, testcase.expectedValue)
			}
		}
	}
}
