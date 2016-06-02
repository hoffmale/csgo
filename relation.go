package csgo

import (
	"fmt"
	"strings"
	"crypto/md5"
	"io"
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
// rightRelation is the right relation for the hash join
// joinType specifies the kind of hash join (inner, outer, semi ...)
// compType specifies the comparison type for the join.
// The join may be executed on one or more columns of each relation.
func (r Relation) HashJoin(col1 []AttrInfo,
	rightRelation Relationer, col2 []AttrInfo,
	joinType JoinType, compType Comparison) Relationer {


	left, right, cols := Rebuilding(col1, col2)
	// Hashing
	var hash interface {} // map[][]int{}
	switch col1[left].Type {
		case INT:
			hash = make(map[int][]int{}, 0)
		case FLOAT:
			hash = make(map[float64][]int{}, 0)
		case STRING:
			hash = make(map[string][]int{}, 0)
	}

	for i := 0; i < r.GetNumRows(); i++ {
		hash = append(hash[r.Columns[left].GetRow()], i)

	}

	// 1. Neue Relation erstellen
	var rout Relation //{"HashJoin", cols}
	rout.Name = "HASH"
	rout.Columns = cols

	// Join :-
	for index := 0; index < rightRelation.GetNumRows(); index++ {
		refs, ok := hash[rightRelation.Columns[right].GetRow(index)]

		if ok {
			for _, ref := range refs {
				for j, _ := range rout.Columns {
					if j < len(col1) {
						rout.Columns[j] = append(rout.Columns[j], r.Columns[j].GetRow(ref))
					} else {
						if j - len(col1) < right {
							rout.Columns[j] = append(rout.Columns[j], rightRelation.Columns[j - len(col1)].GetRow(index))
						} else {
							if j - len(col1) > right {
								rout.Columns[j] = append(rout.Columns[j - 1], rightRelation.Columns[j - len(col1)].GetRow(index))
							}
						}
					}
				}
			}
		}
	}

	return rout
}

// Aggregate should implement the grouping and aggregation of columns.
// groupBy specifies on which columns it should be grouped.
// aggregate defines the column on which the aggrFunc should be applied.
// currently not implemented

func hash_group(args []interface) {
	h := md5.New()
	for _, arg := range args {
        io.WriteString(h, arg)
    }
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (r Relation) Aggregate(groupBy []AttrInfo, aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	var cols []Column
	for j := 0; j < len(r.Columns); j++ {
		cols = append(cols, NewColumn(r.Columns[i].Signature))
	}

	var rout Relation
	rout.Name = "Aggregate"
	rout.Columns = cols

	hash_table := make(map[int][]interface{}, 0)
	hash_group := make(map[int][]interface{}, 0)
	for i := 0; i < r.GetNumRows(); i++ {
		key := hash_func(i)
		val, ok := hash[key]

		if ok {
			// update the matching row
			hash_table[key] = []interface {}
			hash_group[key] = []interface {}
		}

		for j := 0; j < len(r.Columns); j++ {
			// XXX
			/* if ok {
				if ! contains(groupBy, r.Columns[j].Signature) {
					if r.Columns[j].Signature == aggregate {
						rout.Columns[j] = aggrFunc(r.Columns[j])
						//hash[key] = append(hash[key], aggrFunc(r.Columns[j]))
					} else {
						hash[key] = append(hash[key], r.Columns[j].GetRow(i))
					}
				}
			} */
			// XXX

			if ! contains(groupBy, r.Columns[j].Signature) {
				if r.Columns[j].Signature == aggregate {
					hash_table[key] = append(hash_table[key], aggrFunc(r.Columns[j]))
				} else {
					hash_table[key] = append(hash_table[key], r.Columns[j].GetRow(i))
				}
			} else {
				_, ok := hash_group[key]
				if ! ok {
					hash_group[key] = append(hash_group[key], r.Columns[j].GetRow(i))
				}
			}
		}
	}

	for key, gc := range hash_group {
		i:= 0

		for _, val := range gc {
			rout.Columns[i].AddRow(r.Columns[i].Signature.Type, val)
			i = i + 1
		}

		for _, val := range hash_table[key] {
			rout.Columns[i].AddRow(r.Columns[i].Signature.Type, val)
			i = i + 1
		}
	}

	return rout
}
