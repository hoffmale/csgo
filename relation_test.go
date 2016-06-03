package csgo

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestRelationLoad_DataMatch(t *testing.T) {
	r := Relation{Name: "testRel1", Columns: []Column{
		NewColumn(AttrInfo{Name: "col1", Type: INT, Enc: NOCOMP}),
		NewColumn(AttrInfo{Name: "col2", Type: STRING, Enc: NOCOMP}),
	}}

	// create temp file
	file, err := os.Create("temp.csv")

	if err != nil {
		t.Error(err)
		t.Skip()
	}
	file.WriteString("1,a\n2,b\n3,c\n4,d\n5,e")
	file.Close()

	defer os.Remove("temp.csv")

	r.Load("temp.csv", ',')

	//cols, _ := r.GetRawData()
	cols := r.Columns

	if len(cols) != 2 {
		t.Errorf("expected 2 columns, found %d", len(cols))
		t.Fail()
	} else {
		if !reflect.DeepEqual([]int{1, 2, 3, 4, 5}, cols[0].GetRawData()) || !reflect.DeepEqual([]string{"a", "b", "c", "d", "e"}, cols[1].GetRawData()) {
			t.Error("test file content does not match up with relation content")
			t.Log(cols[0])
			t.Log(cols[1])
			t.Fail()
		}
	}
}

func TestRelationLoad_FileNotExisting(t *testing.T) {
	// error catcher
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("loading a not existing file succeeded")
			t.Fail()
		}
	}()

	os.Remove("_temp.csv")

	r := Relation{Name: "testRel", Columns: []Column{}}

	r.Load("_temp.csv", ',')
}

func TestRelationLoad_MalformedCSVFile(t *testing.T) {
	cases := []string{
		"a,b,c\nd,e,f,g",
		"a,b,c,d",
		"a,b",
		"a,b,c,",
		",,,,",
	}

	for _, fileContent := range cases {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("loading a malformed file succeeded")
				t.Fail()
			}
		}()

		writeTestFile("_temp.csv", fileContent)
		r := Relation{Name: "testRel", Columns: []Column{
			NewColumn(AttrInfo{Name: "testCol1", Type: STRING, Enc: NOCOMP}),
			NewColumn(AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}),
			NewColumn(AttrInfo{Name: "testCol3", Type: STRING, Enc: NOCOMP}),
		}}

		r.Load("_temp.csv", ',')
	}
}

func TestRelationScan(t *testing.T) {
	r := Relation{
		Name: "testRel",
		Columns: []Column{
			NewColumnWithData(AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, []int{1, 2, 3}),
			NewColumnWithData(AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, []string{"testVal1", "testVal2", "testVal3"}),
			NewColumnWithData(AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, []float64{1.0, 2.0, 3.0}),
		},
	}

	cases := []struct {
		attrs []AttrInfo
		cols  []interface{}
	}{
		{attrs: []AttrInfo{{Name: "testCol1", Type: INT, Enc: NOCOMP}}, cols: []interface{}{[]int{1, 2, 3}}},
		{attrs: []AttrInfo{{Name: "testCol2", Type: STRING, Enc: NOCOMP}}, cols: []interface{}{[]string{"testVal1", "testVal2", "testVal3"}}},
		{attrs: []AttrInfo{{Name: "testCol1", Type: INT, Enc: NOCOMP}, {Name: "testCol3", Type: FLOAT, Enc: NOCOMP}}, cols: []interface{}{[]int{1, 2, 3}, []float64{1.0, 2.0, 3.0}}},
	}

	for _, c := range cases {
		result := r.Scan(c.attrs)

		cols, sigs := result.GetRawData()

		if len(cols) != len(c.cols) {
			t.Errorf("expected %d columns, found %d instead", len(c.cols), len(cols))
			t.Fail()
		}

		if !reflect.DeepEqual(cols, c.cols) {
			t.Errorf("columns do not match")
			t.Log(cols)
			t.Fail()
		}

		if len(sigs) != len(c.attrs) {
			t.Errorf("expected %d signatures, found %d instead", len(c.attrs), len(sigs))
			t.Fail()
		}

		if !reflect.DeepEqual(sigs, c.attrs) {
			t.Error("signatures do not match")
			t.Log(c.attrs)
			t.Log(sigs)
			t.Fail()
		}
	}
}

func TestRelationSelect(t *testing.T) {
	// TODO: write cases
	r := Relation{Name: "testRel", Columns: []Column{
		NewColumnWithData(AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
		NewColumnWithData(AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, []string{"val1", "val2", "val1", "val2", "val1", "val2", "val1", "val2", "val1", "val2"}),
		NewColumnWithData(AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1}),
	}}

	cases := []struct {
		col    AttrInfo
		comp   Comparison
		value  interface{}
		result []interface{}
	}{
		// INT cases
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: EQ, value: int(3), result: []interface{}{[]int{3}, []string{"val2"}, []float64{0.7}}},
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: NEQ, value: int(5), result: []interface{}{[]int{0, 1, 2, 3, 4, 6, 7, 8, 9}, []string{"val1", "val2", "val1", "val2", "val1", "val1", "val2", "val1", "val2"}, []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.4, 0.3, 0.2, 0.1}}},
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: LT, value: int(4), result: []interface{}{[]int{0, 1, 2, 3}, []string{"val1", "val2", "val1", "val2"}, []float64{1.0, 0.9, 0.8, 0.7}}},
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: LEQ, value: int(2), result: []interface{}{[]int{0, 1, 2}, []string{"val1", "val2", "val1"}, []float64{1.0, 0.9, 0.8}}},
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: GT, value: int(8), result: []interface{}{[]int{9}, []string{"val2"}, []float64{0.1}}},
		{col: AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, comp: GEQ, value: int(6), result: []interface{}{[]int{6, 7, 8, 9}, []string{"val1", "val2", "val1", "val2"}, []float64{0.4, 0.3, 0.2, 0.1}}},
		// FLOAT cases
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: EQ, value: float64(0.4), result: []interface{}{[]int{6}, []string{"val1"}, []float64{0.4}}},
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: NEQ, value: float64(0.2), result: []interface{}{[]int{0, 1, 2, 3, 4, 5, 6, 7, 9}, []string{"val1", "val2", "val1", "val2", "val1", "val2", "val1", "val2", "val2"}, []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.1}}},
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: LT, value: float64(0.5), result: []interface{}{[]int{6, 7, 8, 9}, []string{"val1", "val2", "val1", "val2"}, []float64{0.4, 0.3, 0.2, 0.1}}},
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: LEQ, value: float64(0.3), result: []interface{}{[]int{7, 8, 9}, []string{"val2", "val1", "val2"}, []float64{0.3, 0.2, 0.1}}},
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: GT, value: float64(0.6), result: []interface{}{[]int{0, 1, 2, 3}, []string{"val1", "val2", "val1", "val2"}, []float64{1.0, 0.9, 0.8, 0.7}}},
		{col: AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, comp: GEQ, value: float64(0.7), result: []interface{}{[]int{0, 1, 2, 3}, []string{"val1", "val2", "val1", "val2"}, []float64{1.0, 0.9, 0.8, 0.7}}},
		// STRING cases
		{col: AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, comp: EQ, value: "val1", result: []interface{}{[]int{0, 2, 4, 6, 8}, []string{"val1", "val1", "val1", "val1", "val1"}, []float64{1.0, 0.8, 0.6, 0.4, 0.2}}},
		{col: AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, comp: NEQ, value: "val1", result: []interface{}{[]int{1, 3, 5, 7, 9}, []string{"val2", "val2", "val2", "val2", "val2"}, []float64{0.9, 0.7, 0.5, 0.3, 0.1}}},
		// invalid comp func
		{col: AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, comp: LT, value: "val1", result: []interface{}{[]int{}, []string{}, []float64{}}},
	}

	for testcaseID, testcase := range cases {
		result := r.Select(testcase.col, testcase.comp, testcase.value)
		resultData, _ := result.GetRawData()

		if !reflect.DeepEqual(testcase.result, resultData) {
			t.Errorf("testcase %d: result %v is not matching expectations %v", testcaseID, resultData, testcase.result)
			t.Fail()
		}
	}
}

func TestRelationGetRawData(t *testing.T) {
	cases := []struct {
		rel  Relation
		sigs []AttrInfo
		cols []interface{}
	}{
		{
			rel:  Relation{Name: "testRel1", Columns: nil},
			sigs: nil,
			cols: nil,
		},
		{
			rel:  Relation{Name: "testRel2", Columns: make([]Column, 0)},
			sigs: nil,
			cols: nil,
		},
		{
			rel: Relation{
				Name: "testRel3",
				Columns: []Column{
					NewColumnWithData(AttrInfo{Name: "testCol1", Type: INT, Enc: NOCOMP}, []int{1, 2, 3}),
					NewColumnWithData(AttrInfo{Name: "testCol2", Type: STRING, Enc: NOCOMP}, []string{"testValue1", "testVal2", "testValutas3"}),
					NewColumnWithData(AttrInfo{Name: "testCol3", Type: FLOAT, Enc: NOCOMP}, []float64{1.0, 2.0, 3.0}),
				},
			},
			sigs: []AttrInfo{
				{Name: "testCol1", Type: INT, Enc: NOCOMP},
				{Name: "testCol2", Type: STRING, Enc: NOCOMP},
				{Name: "testCol3", Type: FLOAT, Enc: NOCOMP},
			},
			cols: []interface{}{
				[]int{1, 2, 3},
				[]string{"testValue1", "testVal2", "testValutas3"},
				[]float64{1.0, 2.0, 3.0},
			},
		},
	}

	for _, c := range cases {
		cols, sigs := c.rel.GetRawData()

		if !reflect.DeepEqual(cols, c.cols) {
			t.Errorf("Error in case '%s': Expected cols [len=%d] to match [len=%d](expected)", c.rel.Name, len(cols), len(c.cols))
			t.Log(cols)
			t.Log(sigs)
			t.Fail()
		}

		if !reflect.DeepEqual(sigs, c.sigs) {
			t.Errorf("Error in case '%s': Expected sig [Len=%d] to match [Len=%d](expected)", c.rel.Name, len(sigs), len(c.sigs))
			t.Log(cols)
			t.Log(sigs)
			t.Fail()
		}
	}
}

func TestRelationMergeSort(t *testing.T) {
	cases := []struct {
		left      Relation
		right     Relation
		leftCols  []AttrInfo
		rightCols []AttrInfo
		output    Relation
		joinType  JoinType
		compType  Comparison
	}{
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{2, 3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 3}),
				},
			},
			joinType: INNER,
			compType: EQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", FLOAT, NOCOMP, 0}, []float64{0.0, 0.1, 0.2, 0.3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", FLOAT, NOCOMP, 0}, []float64{0.0, 0.2, 0.4, 0.6})}},
			leftCols:  []AttrInfo{{"leftCol1", FLOAT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", FLOAT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", FLOAT, NOCOMP, 0}, []float64{0.0, 0.2}),
					NewColumnWithData(AttrInfo{"right.rightCol1", FLOAT, NOCOMP, 0}, []float64{0.0, 0.2}),
				},
			},
			joinType: INNER,
			compType: EQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", STRING, NOCOMP, 0}, []string{"a", "aa", "b", "bb", "c", "cc"})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", STRING, NOCOMP, 0}, []string{"a", "ab", "b", "bb", "c", "cb"})}},
			leftCols:  []AttrInfo{{"leftCol1", STRING, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", STRING, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", STRING, NOCOMP, 0}, []string{"a", "b", "bb", "c"}),
					NewColumnWithData(AttrInfo{"right.rightCol1", STRING, NOCOMP, 0}, []string{"a", "b", "bb", "c"}),
				},
			},
			joinType: INNER,
			compType: EQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 2, 2, 3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4, 3, 4, 4}),
				},
			},
			joinType: INNER,
			compType: LT,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 2, 2, 2, 3, 3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4, 2, 3, 4, 3, 4}),
				},
			},
			joinType: INNER,
			compType: LEQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2}),
				},
			},
			joinType: INNER,
			compType: GT,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{2, 3, 3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 2, 3}),
				},
			},
			joinType: INNER,
			compType: GEQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 2, 2, 3, 3}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 3, 4, 3, 4, 2, 4}),
				},
			},
			joinType: INNER,
			compType: NEQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 5, 5})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 1, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 1, 1, 2, 2, 5, 5, 1, 1, 2, 2, 5, 5}),
				},
			},
			joinType: INNER,
			compType: GEQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 5, 5})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{2, 2, 5, 5, 2, 2, 5, 5, 1, 1, 5, 5, 1, 1, 5, 5, 1, 1, 2, 2, 5, 5, 1, 1, 2, 2, 5, 5, 1, 1, 2, 2, 5, 5, 1, 1, 2, 2, 5, 5, 1, 1, 2, 2, 1, 1, 2, 2}),
				},
			},
			joinType: INNER,
			compType: NEQ,
		},
		{
			left: Relation{
				Name: "left",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}),
					NewColumnWithData(AttrInfo{"leftCol2", INT, NOCOMP, 0}, []int{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}),
				},
			},
			right: Relation{
				Name: "right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{1, 2, 3, 4, 5, 5}),
					NewColumnWithData(AttrInfo{"rightCol2", INT, NOCOMP, 0}, []int{1, 2, 1, 2, 1, 2}),
				},
			},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}, {"leftCol2", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}, {"rightCol2", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left x right",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3, 4, 5, 5}),
					NewColumnWithData(AttrInfo{"left.leftCol2", INT, NOCOMP, 0}, []int{1, 2, 1, 2, 1, 2}),
					NewColumnWithData(AttrInfo{"right.rightCol1", INT, NOCOMP, 0}, []int{1, 2, 3, 4, 5, 5}),
					NewColumnWithData(AttrInfo{"right.rightCol2", INT, NOCOMP, 0}, []int{1, 2, 1, 2, 1, 2}),
				},
			},
			joinType: INNER,
			compType: EQ,
		},
		{
			left:      Relation{Name: "left", Columns: []Column{NewColumnWithData(AttrInfo{"leftCol1", INT, NOCOMP, 0}, []int{1, 2, 3, 4, 5})}},
			right:     Relation{Name: "right", Columns: []Column{NewColumnWithData(AttrInfo{"rightCol1", INT, NOCOMP, 0}, []int{1, 1, 2, 2, 4, 5})}},
			leftCols:  []AttrInfo{{"leftCol1", INT, NOCOMP, 0}},
			rightCols: []AttrInfo{{"rightCol1", INT, NOCOMP, 0}},
			output: Relation{
				Name: "left (x right)",
				Columns: []Column{
					NewColumnWithData(AttrInfo{"left.leftCol1", INT, NOCOMP, 0}, []int{1, 2, 4, 5}),
				},
			},
			joinType: SEMI,
			compType: EQ,
		},
	}

	for _, testCase := range cases {
		output := testCase.left.MergeJoin(testCase.leftCols, testCase.right, testCase.rightCols, testCase.joinType, testCase.compType)

		output.Print()

		if !reflect.DeepEqual(output, testCase.output) {
			t.Fail()
			fmt.Println("fail")
		}
	}
}
