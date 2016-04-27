package csgo

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Load should load and insert the data of a CSV file into the column store.
// csvFile is the path to the CSV File.
// separator is separator character used in the file.
func (r Relation) Load(csvFile string, separator rune) {
	file, err := os.Open(csvFile)

	if err != nil {
		fmt.Print("error [Relation.Load]: ")
		fmt.Println(err)
		return
	}

	defer file.Close()

	//scanner := bufio.NewScanner(file)
	reader := bufio.NewReaderSize(file, 64*1024)
	lineNo := 0

	//for scanner.Scan() {
	for err == nil {
		line, err := reader.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		//line := scanner.Text()
		if err != nil && err != io.EOF {
			fmt.Print("error [Relation.Load:37]: ")
			fmt.Print(err)
			return
		}
		lineNo += 1

		fields := strings.Split(line, string(separator))

		if len(fields) != len(r.Columns) {
			fmt.Print("error [Relation.Load:48]: ")
			fmt.Printf("Found row with %d fields, relation contains %d fields instead", len(fields), len(r.Columns))
			fmt.Println("")
			return
		}

		for index, fieldValue := range fields {
			switch r.Columns[index].Signature.Type {
			case INT:
				value, err2 := strconv.Atoi(fieldValue)
				if err2 != nil {
					fmt.Print("error [Relation.Load:59]: ")
					fmt.Println(err2)
					return
				}
				r.Columns[index].Data = append(r.Columns[index].Data.([]int), value)
			case STRING:
				r.Columns[index].Data = append(r.Columns[index].Data.([]string), fieldValue)
			case FLOAT:
				value, err2 := strconv.ParseFloat(fieldValue, 64)
				if err2 != nil {
					fmt.Print("error [Relation.Load:69]: ")
					fmt.Println(err2)
					return
				}
				r.Columns[index].Data = append(r.Columns[index].Data.([]float64), value)
			}
		}

		//line, err = reader.ReadString('\n')
		if err == io.EOF {
			break
		}
	}

	//if err = scanner.Err(); err != nil {
	if err != nil && err != io.EOF {
		fmt.Print("error [Relation.Load:82]: ")
		fmt.Print(err)
		return
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

		newCol := Column{Signature: cols.Signature, Data: nil}
		switch cols.Signature.Type {
		case INT:
			newCol.Data = []int{}
		case STRING:
			newCol.Data = []string{}
		case FLOAT:
			newCol.Data = []float64{}
		}
		result.Columns = append(result.Columns, newCol)
	}

	copyRow := func(rowIndex int) {
		for colIndex := range r.Columns {
			switch r.Columns[colIndex].Signature.Type {
			case INT:
				result.Columns[colIndex].Data = append(result.Columns[colIndex].Data.([]int), (r.Columns[colIndex].Data.([]int))[rowIndex])
			case STRING:
				result.Columns[colIndex].Data = append(result.Columns[colIndex].Data.([]string), (r.Columns[colIndex].Data.([]string))[rowIndex])
			case FLOAT:
				result.Columns[colIndex].Data = append(result.Columns[colIndex].Data.([]float64), (r.Columns[colIndex].Data.([]float64))[rowIndex])
			}
		}
	}

	switch filterColumn.Signature.Type {
	case INT:
		compValue := compVal.(int)
		for rowIndex, rowValue := range filterColumn.Data.([]int) {
			valid := false
			switch comp {
			case EQ:
				valid = rowValue == compValue
			case NEQ:
				valid = rowValue != compValue
			case LT:
				valid = rowValue < compValue
			case LEQ:
				valid = rowValue <= compValue
			case GT:
				valid = rowValue > compValue
			case GEQ:
				valid = rowValue >= compValue
			}

			if valid {
				copyRow(rowIndex)
			}
		}
	case FLOAT:
		compValue := compVal.(float64)
		for rowIndex, rowValue := range filterColumn.Data.([]float64) {
			valid := false
			switch comp {
			case EQ:
				valid = rowValue == compValue
			case NEQ:
				valid = rowValue != compValue
			case LT:
				valid = rowValue < compValue
			case LEQ:
				valid = rowValue <= compValue
			case GT:
				valid = rowValue > compValue
			case GEQ:
				valid = rowValue >= compValue
			}

			if valid {
				copyRow(rowIndex)
			}
		}
	case STRING:
		compValue := compVal.(string)
		for rowIndex, rowValue := range filterColumn.Data.([]string) {
			valid := false
			switch comp {
			case EQ:
				valid = rowValue == compValue
			case NEQ:
				valid = rowValue != compValue
				/*case LT:
					valid = rowValue < compValue
				case LEQ:
					valid = rowValue <= compValue
				case GT:
					valid = rowValue > compValue
				case GEQ:
					valid = rowValue >= compValue*/
			}

			if valid {
				copyRow(rowIndex)
			}
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
	data := []previewColumn{}
	for _, col := range r.Columns {
		curCol := previewColumn{}
		curCol.name = col.Signature.Name
		curCol.maxLength = len(col.Signature.Name)
		curCol.alignLeft = false

		switch col.Signature.Type {
		case INT:
			for _, rowValue := range col.Data.([]int) {
				strVal := fmt.Sprintf("%d", rowValue)
				curCol.rows = append(curCol.rows, strVal)

				if len(strVal) > curCol.maxLength {
					curCol.maxLength = len(strVal)
				}
			}
		case STRING:
			for _, rowValue := range col.Data.([]string) {
				curCol.rows = append(curCol.rows, rowValue)

				if len(rowValue) > curCol.maxLength {
					curCol.maxLength = len(rowValue)
				}
			}
			curCol.alignLeft = true
		case FLOAT:
			for _, rowValue := range col.Data.([]float64) {
				strVal := fmt.Sprintf("%f", rowValue)
				curCol.rows = append(curCol.rows, strVal)

				if len(strVal) > curCol.maxLength {
					curCol.maxLength = len(strVal)
				}
			}
		}

		data = append(data, curCol)
	}

	if len(data) <= 0 {
		return
	}

	totalLength := 1
	for _, colData := range data {
		totalLength += 3 + colData.maxLength
	}

	if (totalLength - 4) < len(r.Name) {
		increasePerField := (len(r.Name) - totalLength + 4)

		if increasePerField%len(data) == 0 {
			increasePerField /= len(data)
		} else {
			increasePerField = 1 + increasePerField/len(data)
		}

		for index := range data {
			data[index].maxLength += increasePerField
		}
	}

	lineSep := "+"
	for _, colData := range data {
		lineSep = lineSep + strings.Repeat("-", colData.maxLength+2) + "+"
	}

	fmt.Println("+" + strings.Repeat("-", len(lineSep)-2) + "+")
	indentFront := (len(lineSep) - 2 - len(r.Name)) / 2
	fmt.Print("|" + strings.Repeat(" ", indentFront))
	fmt.Print(r.Name)
	fmt.Println(strings.Repeat(" ", len(lineSep)-indentFront-len(r.Name)-2) + "|")
	fmt.Println(lineSep)

	for _, colData := range data {
		indent := (colData.maxLength - len(colData.name)) / 2
		fmt.Printf("| %s", strings.Repeat(" ", indent)+colData.name)
		fmt.Print(strings.Repeat(" ", colData.maxLength-len(colData.name)-indent+1))
	}
	fmt.Println("|")
	fmt.Println(lineSep)

	for index := 0; index < len(data[0].rows); index++ {
		for _, colData := range data {
			if colData.alignLeft {
				fmt.Printf("| %s", colData.rows[index])
				fmt.Print(strings.Repeat(" ", colData.maxLength-len(colData.rows[index])+1))
			} else {
				fmt.Print("|" + strings.Repeat(" ", colData.maxLength-len(colData.rows[index])+1))
				fmt.Printf("%s ", colData.rows[index])
			}
		}
		fmt.Println("|")
	}

	fmt.Println(lineSep)
	fmt.Println()
}

// GetRawData should return all columns as a slice of slices (columns) with
// the underlying type (int, float, string) in decompressed form and the
// corresponding meta information.
func (r Relation) GetRawData() (cols []interface{}, sigs []AttrInfo) {
	for _, col := range r.Columns {
		cols = append(cols, col.Data)
		sigs = append(sigs, col.Signature)
	}
	return cols, sigs
}

// HashJoin should implement the hash join operator between two relations.
// joinType specifies the kind of hash join (inner, outer, semi ...)
// The join may be executed on one or more columns of each relation.
// currently not implemented
func (r Relation) HashJoin(col1 []AttrInfo, input2 []Column, col2 []AttrInfo, joinType JoinType) Relationer {
	return nil
}

// Aggregate should implement the grouping and aggregation of columns.
// groupBy specifies on which columns it should be grouped.
// aggregate defines the column on which the aggrFunc should be applied.
// currently not implemented
func (r Relation) Aggregate(groupBy []AttrInfo, aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	return nil
}
