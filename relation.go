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
	result := Relation{Name: r.Name, Columns: []Column{}}

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
		LT: func(value1 interface{}, value2 interface{}) bool {
			return strings.Compare(value1.(string), value2.(string)) < 0
		},
		GT: func(value1 interface{}, value2 interface{}) bool {
			return strings.Compare(value1.(string), value2.(string)) > 0
		},
		LEQ: func(value1 interface{}, value2 interface{}) bool {
			return strings.Compare(value1.(string), value2.(string)) <= 0
		},
		GEQ: func(value1 interface{}, value2 interface{}) bool {
			return strings.Compare(value1.(string), value2.(string)) >= 0
		},
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
	result := Relation{Name: r.Name, Columns: []Column{}}

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

// SortOrder is an enumeration type for all supported sorting modes
type SortOrder int

const (
	// ASC specifies ascending sorting order
	ASC SortOrder = iota
	// DESC specifies descending sorting order
	DESC
)

// MergeSort creates a new ordered Relation
// columns specifies the columns which should be sorted (in order of sorting)
// sortOrder specifies the sorting order
func (r Relation) MergeSort(columns []AttrInfo, sortOrder SortOrder) Relationer {
	type SortData struct {
		Column  *Column
		Compare CompFunc
		Equals  CompFunc
	}

	sortData := make([]SortData, len(columns))
	output := Relation{}

	compare := func(aIndex int, bIndex int) bool {
		for _, curStep := range sortData {
			aValue, _ := curStep.Column.GetRow(aIndex)
			bValue, _ := curStep.Column.GetRow(bIndex)

			if curStep.Compare(aValue, bValue) {
				return true
			}
			if !curStep.Equals(aValue, bValue) {
				return false
			}
		}
		// rows are identical, order doesn't matter
		return true
	}

	merge := func(listA []int, listB []int) []int {
		output := make([]int, len(listA)+len(listB))

		aIndex, bIndex := 0, 0

		for aIndex < len(listA) && bIndex < len(listB) {
			if !compare(listA[aIndex], listB[bIndex]) {
				output[aIndex+bIndex] = listB[bIndex]
				bIndex++
			} else {
				output[aIndex+bIndex] = listA[aIndex]
				aIndex++
			}
		}

		for ; aIndex < len(listA); aIndex++ {
			output[aIndex+bIndex] = listA[aIndex]
		}

		for ; bIndex < len(listB); bIndex++ {
			output[aIndex+bIndex] = listB[bIndex]
		}

		return output
	}

	var mergeSort func([]int) []int
	mergeSort = func(list []int) []int {
		if len(list) == 1 {
			return list
		}

		return merge(mergeSort(list[:len(list)/2]), mergeSort(list[len(list)/2:]))
	}

	setup := func() {
		// setup sortData
		compType := LT
		if sortOrder == DESC {
			compType = GT
		}

		for index, signature := range columns {
			for colIndex, col := range r.Columns {
				if col.Signature == signature {
					sortData[index] = SortData{
						Column:  &r.Columns[colIndex],
						Equals:  compFuncs[signature.Type][EQ],
						Compare: compFuncs[signature.Type][compType],
					}
				}
			}
		}

		// init output Relation
		output.Name = r.Name + "(sorted)"
		output.Columns = []Column{}

		for _, col := range r.Columns {
			output.Columns = append(output.Columns, NewColumn(col.Signature))
		}
	}

	createIota := func(length int) []int {
		output := make([]int, length)

		for i := 0; i < length; i++ {
			output[i] = i
		}

		return output
	}

	copyColValues := func(source *Column, dest *Column, order []int) {
		for _, index := range order {
			value, _ := source.GetRow(index)
			dest.AddRow(source.Signature.Type, value)
		}
	}

	copyValues := func(indices []int) {
		for colIndex := range r.Columns {
			copyColValues(&r.Columns[colIndex], &output.Columns[colIndex], indices)
		}
	}

	setup()
	copyValues(mergeSort(createIota(r.Columns[0].GetNumRows())))
	return output
}

// MergeJoin should implement the merge join operator between two relations.
// joinType specifies the kind of hash join
func (r Relation) MergeJoin(leftCols []AttrInfo, rightRelation Relationer, rightCols []AttrInfo, joinType JoinType, compType Comparison) Relationer {
	right, isRelation := rightRelation.(Relation)

	if !isRelation {
		panic("unknown relation type")
		// TODO: implement using Relationer.GetRawData()
	}

	type MergeData struct {
		Left    *Column
		Right   *Column
		Compare CompFunc
		Lesser  CompFunc
		Equals  CompFunc
	}

	right = right.MergeSort(rightCols, ASC).(Relation)
	left := r.MergeSort(leftCols, ASC).(Relation)
	output := Relation{Columns: []Column{}}

	leftIndices := []int{}
	rightIndices := []int{}

	leftRow, rightRow := 0, 0
	maxLeftRows := left.Columns[0].GetNumRows()
	maxRightRows := right.Columns[0].GetNumRows()
	var mergeData []MergeData

	addOutputCols := func(base *Relation, tableName string, nullable bool) {
		if nullable {
			panic("NULL values not implemented")
		}
		for _, col := range base.Columns {
			signature := AttrInfo{Name: tableName + "." + col.Signature.Name, Enc: col.Signature.Enc, Type: col.Signature.Type}
			output.Columns = append(output.Columns, NewColumn(signature))
		}
	}

	getMergeData := func() []MergeData {
		output := []MergeData{}

		for sigIndex, signature := range leftCols {
			entry := MergeData{}

			for colIndex, col := range left.Columns {
				if col.Signature == signature {
					entry.Left = &left.Columns[colIndex]
					break
				}
			}

			for colIndex, col := range right.Columns {
				if col.Signature == rightCols[sigIndex] {
					entry.Right = &right.Columns[colIndex]
					break
				}
			}

			if entry.Left == nil || entry.Right == nil {
				panic("column not found")
			}

			entry.Equals = compFuncs[signature.Type][EQ]
			entry.Lesser = compFuncs[signature.Type][LT]

			output = append(output, entry)
		}

		return output
	}

	isEqual := func(leftIndex int, rightIndex int) bool {
		for _, entry := range mergeData {
			leftValue, _ := entry.Left.GetRow(leftIndex)
			rightValue, _ := entry.Right.GetRow(rightIndex)
			if !entry.Equals(leftValue, rightValue) {
				return false
			}
		}
		return true
	}

	isLesser := func(leftIndex, rightIndex int) bool {
		for _, entry := range mergeData {
			leftValue, _ := entry.Left.GetRow(leftIndex)
			rightValue, _ := entry.Right.GetRow(rightIndex)

			if entry.Lesser(leftValue, rightValue) {
				return true
			}
			if !entry.Equals(leftValue, rightValue) {
				return false
			}
		}
		// entries are equal
		return false
	}

	getNextRow := func(compare func(int, int) bool) int {
		nextRow := rightRow + 1

		for nextRow < maxRightRows && compare(leftRow, nextRow) {
			nextRow++
		}

		return nextRow
	}

	innerJoin := func() ([]int, []int) {
		mergeData = getMergeData()

		for leftRow < maxLeftRows && rightRow < maxRightRows {
			if isEqual(leftRow, rightRow) {
				// leftValue == rightValue
				nextRow := getNextRow(isEqual)

				switch compType {
				case GT:
					for i := 0; i < rightRow; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case GEQ:
					for i := 0; i < nextRow; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case LT:
					for i := nextRow; i < maxRightRows; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case LEQ:
					for i := rightRow; i < maxRightRows; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case EQ:
					for i := rightRow; i < nextRow; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case NEQ:
					for i := 0; i < rightRow; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
					for i := nextRow; i < maxRightRows; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				}

				leftRow++
			} else if isLesser(leftRow, rightRow) {
				// leftValue < rightValue
				switch compType {
				case GT, GEQ:
					for i := 0; i < rightRow; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case LT, LEQ:
					for i := rightRow; i < maxRightRows; i++ {
						leftIndices = append(leftIndices, leftRow)
						rightIndices = append(rightIndices, i)
					}
				case NEQ:
					for i := 0; i < maxRightRows; i++ {
						if !isEqual(leftRow, i) {
							leftIndices = append(leftIndices, leftRow)
							rightIndices = append(rightIndices, i)
						}
					}
				}

				leftRow++
			} else {
				// leftValue > rightValue
				nextRow := getNextRow(func(l, r int) bool { return !isEqual(l, r) && !isLesser(l, r) })

				switch compType {
				case NEQ:
					// do something?
				}

				rightRow = nextRow
			}
		}

		return leftIndices, rightIndices
	}

	semiJoin := func() {
		mergeData = getMergeData()

		if compType != EQ {
			panic("semi join only supports equality comparison")
		}

		for leftRow < maxLeftRows && rightRow < maxRightRows {
			if isEqual(leftRow, rightRow) {
				leftIndices = append(leftIndices, leftRow)

				leftRow++
			} else if isLesser(leftRow, rightRow) {
				leftRow++
			} else {
				nextRow := getNextRow(func(l, r int) bool { return !isEqual(l, r) && !isLesser(l, r) })
				rightRow = nextRow
			}
		}
	}

	copyColumn := func(source *Column, dest *Column, indices []int) {
		for _, row := range indices {
			value, _ := source.GetRow(row)
			dest.AddRow(source.Signature.Type, value)
		}
	}

	copyLeftValues := func(indices []int) {
		for colIndex := range left.Columns {
			copyColumn(&left.Columns[colIndex], &output.Columns[colIndex], indices)
		}
	}

	copyRightValues := func(indices []int) {
		numLeftCols := len(left.Columns)
		for colIndex := range right.Columns {
			copyColumn(&right.Columns[colIndex], &output.Columns[numLeftCols+colIndex], indices)
		}
	}

	switch joinType {
	case INNER:
		output.Name = r.Name + " x " + rightRelation.(Relation).Name
		addOutputCols(&left, r.Name, false)
		addOutputCols(&right, rightRelation.(Relation).Name, false)
		innerJoin()
		copyLeftValues(leftIndices)
		copyRightValues(rightIndices)
		break
	case SEMI:
		output.Name = r.Name + " (x " + rightRelation.(Relation).Name + ")"
		addOutputCols(&left, r.Name, false)
		semiJoin()
		copyLeftValues(leftIndices)
		break
	case LEFTOUTER:
		// handle null values on left
		panic("NULL values not implemented")
	case RIGHTOUTER:
		// handle null values on right
		panic("NULL values not implemented")
	default:
		panic("unknown JoinType")
	}

	return output
}
