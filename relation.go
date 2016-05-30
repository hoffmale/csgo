package csgo

import (
	"fmt"
	"strings"
)

// Load should load and insert the data of a CSV file into the column store.
// csvFile is the path to the CSV File.
// separator is separator character used in the file.
func (r Relation) Load(csvFile string, separator rune) {
	file, err := CreateFileReader(csvFile)

	if err != nil {
		panic(fmt.Sprintf("error creating FileReader: %#v\n", err))
	}

	defer file.Close()

	for !file.EOFReached {
		line, err := file.ReadLine()

		if err != nil {
			fmt.Print("error reading file: ")
			fmt.Print(err)
			return
		}

		fields := strings.Split(line, string(separator))

		if len(fields) != len(r.Columns) {
			panic(fmt.Sprintf("error during parsing: Found row with %d fields, relation contains %d fields instead (the file might be corrupted!)", len(fields), len(r.Columns)))
		}

		for index, fieldValue := range fields {
			r.Columns[index].ImportRow(fieldValue)
		}
	}
}

// Scan should simply return the specified columns of the relation.
func (r Relation) Scan(colList []AttrInfo) Relationer {
	result := Relation{Name: r.Name + " (scanned)", Columns: []Column{}}

	for _, col := range r.Columns {
		included := false

		for _, colHeader := range colList {
			included = included || colHeader == col.Signature
		}

		if included {
			result.Columns = append(result.Columns, col)
		}
	}

	if len(result.Columns) > 0 {
		return result
	}
	return nil
}

// CompFunc represents a function that does a comparison on 2 values
type CompFunc (func(interface{}, interface{}) bool)

var compFuncs = map[DataTypes]map[Comparison]CompFunc{
	INT: map[Comparison]CompFunc{
		LT:  func(value1 interface{}, value2 interface{}) bool { return value1.(int) < value2.(int) },
		GT:  func(value1 interface{}, value2 interface{}) bool { return value1.(int) > value2.(int) },
		LEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(int) <= value2.(int) },
		GEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(int) >= value2.(int) },
		EQ:  func(value1 interface{}, value2 interface{}) bool { return value1.(int) == value2.(int) },
		NEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(int) != value2.(int) },
	},
	FLOAT: map[Comparison]CompFunc{
		LT:  func(value1 interface{}, value2 interface{}) bool { return value1.(float64) < value2.(float64) },
		GT:  func(value1 interface{}, value2 interface{}) bool { return value1.(float64) > value2.(float64) },
		LEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(float64) <= value2.(float64) },
		GEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(float64) >= value2.(float64) },
		EQ:  func(value1 interface{}, value2 interface{}) bool { return value1.(float64) == value2.(float64) },
		NEQ: func(value1 interface{}, value2 interface{}) bool { return value1.(float64) != value2.(float64) },
	},
	STRING: map[Comparison]CompFunc{
		EQ:  func(value1 interface{}, value2 interface{}) bool { return value1.(string) == value2.(string) },
		NEQ: func(value1 interface{}, value2 interface{}) bool { return !(value1.(string) == value2.(string)) },
	},
}

// Select should return a filtered collection of records defined by predicate
// arguments (col, comp, compVal) of one relation.
// col represents the column used for comparison.
// comp defines the type of comparison.
// compVal is the value used for the comparison.
func (r Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer {
	result := Relation{Name: r.Name + " (selection)", Columns: []Column{}}

	var filterColumn Column
	for _, cols := range r.Columns {
		if cols.Signature == col {
			filterColumn = cols
		}

		newCol := NewColumn(cols.Signature)
		result.Columns = append(result.Columns, newCol)
	}

	copyRow := func(rowIndex int) {
		for colIndex, col := range r.Columns {
			value, _ := col.GetRow(rowIndex)
			result.Columns[colIndex].AddRow(col.Signature.Type, value)
		}
	}

	var compFunc CompFunc
	typeCompFuncs, found := compFuncs[filterColumn.Signature.Type]
	if found {
		compFunc, found = typeCompFuncs[comp]
	}

	if !found {
		fmt.Print("comparison func not found")
		return result
	}

	for rowIndex := 0; rowIndex < filterColumn.GetNumRows(); rowIndex++ {
		value, err := filterColumn.GetRow(rowIndex)
		if err != nil {
			fmt.Printf("encountered unexpected error: %#v", err)
			return nil
		}

		if compFunc(value, compVal) {
			copyRow(rowIndex)
		}
	}
	return result
}

// Print should output the relation to the standard output in record
// representation.
func (r Relation) Print() {
	type previewColumn struct {
		name      string
		rows      []string
		maxLength int
		alignLeft bool
	}

	generatePreview := func() []previewColumn {
		preview := []previewColumn{}

		for _, col := range r.Columns {
			curPreview := previewColumn{
				name:      col.Signature.Name,
				maxLength: len(col.Signature.Name),
				alignLeft: col.Signature.Type == STRING,
				rows:      []string{},
			}

			preview = append(preview, curPreview)
		}

		return preview
	}

	fillPreview := func(preview []previewColumn, startIndex int, endIndex int) []previewColumn {
		for index, col := range r.Columns {
			curPreview := &preview[index]
			for rowIndex := startIndex; rowIndex < endIndex && rowIndex < col.GetNumRows(); rowIndex++ {
				value, _ := col.GetRow(rowIndex)

				strVal := fmt.Sprintf("%v", value)
				curPreview.rows = append(curPreview.rows, strVal)

				if len(strVal) > curPreview.maxLength {
					curPreview.maxLength = len(strVal)
				}
			}
		}

		return preview
	}

	calcTotalWidth := func(preview []previewColumn) int {
		width := 1 // beginning '|'

		for _, curPreview := range preview {
			width += 3 // 2*' ' as padding, 1*'|' as separator to next column
			width += curPreview.maxLength
		}

		return width
	}

	isAdjustmentNeeded := func(width int) bool {
		return len(r.Name)+4 >= width
	}

	adjustWidth := func(preview []previewColumn, baseWidth int) int {
		colIndex := 0
		width := baseWidth

		for isAdjustmentNeeded(width) {
			preview[colIndex].maxLength++
			width++

			colIndex = (colIndex + 1) % len(preview)
		}

		return width
	}

	centerText := func(text string, maxWidth int) string {
		indentBack := (maxWidth - len(text) + 1) / 2
		indentFront := maxWidth - len(text) - indentBack
		if indentBack <= 0 {
			return text
		}

		return strings.Repeat(" ", indentFront) + text + strings.Repeat(" ", indentBack)
	}

	printTableName := func(width int, alone bool) {
		if width < len(r.Name)+4 {
			if !alone {
				return
			}
			width = len(r.Name) + 4
		}

		openingLine := "+" + strings.Repeat("-", width-2) + "+"
		fmt.Println(openingLine)
		fmt.Println("| " + centerText(r.Name, width-4) + " |")

		if alone {
			fmt.Println(openingLine)
		}
	}

	generateRowSeparatorLine := func(preview []previewColumn) string {
		sepLine := "+"

		for _, curPreview := range preview {
			sepLine += strings.Repeat("-", curPreview.maxLength+2) + "+"
		}

		return sepLine
	}

	printColumnHeaders := func(preview []previewColumn) {
		sepLine := generateRowSeparatorLine(preview)
		fmt.Println(sepLine)

		for _, curPreview := range preview {
			fmt.Print("| " + centerText(curPreview.name, curPreview.maxLength) + " ")
		}
		fmt.Println("|")

		fmt.Println(sepLine)
	}

	printRows := func(preview []previewColumn) {
		for index := 0; index < len(preview[0].rows); index++ {
			for _, curPreview := range preview {
				fmt.Print("| ")
				if curPreview.alignLeft {
					fmt.Print(curPreview.rows[index] + strings.Repeat(" ", curPreview.maxLength-len(curPreview.rows[index])))
				} else {
					fmt.Print(strings.Repeat(" ", curPreview.maxLength-len(curPreview.rows[index])) + curPreview.rows[index])
				}
				fmt.Print(" ")
			}
			fmt.Println("|")
		}
	}

	printFooter := func(preview []previewColumn) {
		fmt.Println(generateRowSeparatorLine(preview))
		fmt.Println()
	}

	data := fillPreview(generatePreview(), 0, r.Columns[0].GetNumRows())

	if len(data) <= 0 {
		printTableName(0, true)
		return
	}

	totalWidth := calcTotalWidth(data)
	if isAdjustmentNeeded(totalWidth) {
		totalWidth = adjustWidth(data, totalWidth)
	}

	printTableName(totalWidth, false)
	printColumnHeaders(data)
	printRows(data)
	printFooter(data)
}

// GetRawData should return all columns as a slice of slices (columns) with
// the underlying type (int, float, string) in decompressed form and the
// corresponding meta information.
func (r Relation) GetRawData() (cols []interface{}, sigs []AttrInfo) {
	for _, col := range r.Columns {
		rawValues := col.GetRawData()
		cols = append(cols, rawValues)
		sigs = append(sigs, col.Signature)
	}
	return cols, sigs
}

// HashJoin should implement the hash join operator between two relations.
// joinType specifies the kind of hash join (inner, outer, semi ...)
// The join may be executed on one or more columns of each relation.
// currently not implemented
func (r Relation) HashJoin(col1 []AttrInfo, rightRelation Relationer, col2 []AttrInfo, joinType JoinType,
	compType Comparison) Relationer {
	return nil
}

// Aggregate should implement the grouping and aggregation of columns.
// groupBy specifies on which columns it should be grouped.
// aggregate defines the column on which the aggrFunc should be applied.
// currently not implemented
func (r Relation) Aggregate(aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	return nil
}

// MergeJoin should implement the merge join operator between two relations.
// joinType specifies the kind of hash join
func (r Relation) MergeJoin(col1 AttrInfo, rightRelation Relationer, col2 AttrInfo, joinType JoinType, compType Comparison) Relationer {
	right, isRelation := rightRelation.(Relation)

	if !isRelation {
		panic("unknown relation type")
		// TODO: implement using Relationer.GetRawData()
	}

	output := Relation{Name: r.Name + " x " + right.Name, Columns: []Column{}}

	var leftCol *Column
	var rightCol *Column

	for i := 0; i < len(r.Columns); i++ {
		output.Columns = append(output.Columns, NewColumn(r.Columns[i].Signature))
		if r.Columns[i].Signature == col1 {
			leftCol = &r.Columns[i]
		}
	}

	for i := 0; i < len(right.Columns); i++ {
		output.Columns = append(output.Columns, NewColumn(right.Columns[i].Signature))
		if right.Columns[i].Signature == col2 {
			rightCol = &right.Columns[i]
		}
	}

	rightColOffset := len(r.Columns)

	leftIndex, rightIndex := 0, 0

	createTuple := func(leftIndex int, rightIndex int) {
		for i := 0; i < rightColOffset; i++ {
			value, _ := r.Columns[i].GetRow(leftIndex)
			output.Columns[i].AddRow(r.Columns[i].Signature.Type, value)
		}
		for i := 0; i < len(right.Columns); i++ {
			value, _ := right.Columns[i].GetRow(rightIndex)
			output.Columns[i+rightColOffset].AddRow(right.Columns[i].Signature.Type, value)
		}
	}

	eqFunc := compFuncs[col1.Type][EQ]
	ltFunc := compFuncs[col1.Type][LT]

	for leftIndex < leftCol.GetNumRows() && rightIndex < rightCol.GetNumRows() {
		leftValue, _ := leftCol.GetRow(leftIndex)
		rightValue, _ := rightCol.GetRow(rightIndex)

		if eqFunc(leftValue, rightValue) {
			createTuple(leftIndex, rightIndex)

			nextIndex := rightIndex + 1
			nextValue, _ := rightCol.GetRow(nextIndex)

			for nextIndex < rightCol.GetNumRows() && eqFunc(leftValue, nextValue) {
				createTuple(leftIndex, nextIndex)

				nextIndex++
				nextValue, _ = rightCol.GetRow(nextIndex)
			}

			leftIndex++
		} else if ltFunc(leftValue, rightValue) {
			leftIndex++
		} else {
			rightIndex++
		}
	}

	return output
}
