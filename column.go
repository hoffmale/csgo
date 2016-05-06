package csgo

import (
	"fmt"
	"strconv"
)

// NewColumn creates a new column according to the given AttrInfo
func NewColumn(sig AttrInfo) Column {
	col := Column{Signature: sig}

	switch sig.Enc {
	case NOCOMP:
		col.Data = NewBasicDataStore(sig.Type)
	case RLE:
		col.Data = NewRLEDataStore(sig.Type)
	default:
		col.Data = NewBasicDataStore(sig.Type)
	}
	return col
}

// NewColumnWithData creates a new Column according to the given AttrInfo and fills it with the values in data (must be a slice of the corresponding type).
func NewColumnWithData(sig AttrInfo, data interface{}) Column {
	col := NewColumn(sig)

	switch sig.Type {
	case INT:
		for _, val := range data.([]int) {
			col.AddRow(INT, val)
		}
	case FLOAT:
		for _, val := range data.([]float64) {
			col.AddRow(FLOAT, val)
		}
	case STRING:
		for _, val := range data.([]string) {
			col.AddRow(STRING, val)
		}
	}
	return col
}

// ImportRow imports a string value into the column.
// Useful when parsing text input
func (col *Column) ImportRow(field string) (int, error) {
	switch col.Signature.Type {
	case INT:
		value, err := strconv.Atoi(field)
		if err != nil {
			return -1, err
		}
		return col.AddRow(INT, value)

	case FLOAT:
		value, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return -1, err
		}
		return col.AddRow(FLOAT, value)

	case STRING:
		return col.AddRow(STRING, field)
	}

	// shouldn't happen
	panic("invalid column signature")
}

// AddRow adds a row with the specified value.
// Currently, the value gets appended at the end of the Data slice. This might change in the future.
func (col *Column) AddRow(typ DataTypes, value interface{}) (index int, err error) {
	// catch conversion panics (typ does not match value)
	defer func() {
		if r := recover(); r != nil {
			index = -1
			err = fmt.Errorf("%#v", r)
		}
	}()
	return (col.Data.(DataStore)).AddRow(typ, value)
}

// GetRow returns the value in the given row.
func (col Column) GetRow(index int) (interface{}, error) {
	return (col.Data.(DataStore)).GetRow(index)
}

// GetNumRows returns the number of rows present in the Column.
func (col Column) GetNumRows() int {
	return (col.Data.(DataStore)).GetNumRows()
}

// GetRawData rturns a slice of all values present in the column (in index order).
func (col Column) GetRawData() interface{} {
	switch col.Signature.Type {
	case INT:
		rawValues := []int{}
		for i := 0; i < col.GetNumRows(); i++ {
			value, _ := col.GetRow(i)
			rawValues = append(rawValues, value.(int))
		}
		return rawValues
	case FLOAT:
		rawValues := []float64{}
		for i := 0; i < col.GetNumRows(); i++ {
			value, _ := col.GetRow(i)
			rawValues = append(rawValues, value.(float64))
		}
		return rawValues
	case STRING:
		rawValues := []string{}
		for i := 0; i < col.GetNumRows(); i++ {
			value, _ := col.GetRow(i)
			rawValues = append(rawValues, value.(string))
		}
		return rawValues
	}

	panic("unknown data type")
}
