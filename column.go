package csgo

import (
	"errors"
	"strconv"
)

// NewColumn creates a new column according to the given AttrInfo
func NewColumn(sig AttrInfo) Column {
	col := Column{Signature: sig}

	switch sig.Type {
	case INT:
		col.Data = []int{}
	case FLOAT:
		col.Data = []float64{}
	case STRING:
		col.Data = []string{}
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
func (col *Column) AddRow(typ DataTypes, value interface{}) (int, error) {
	if typ != col.Signature.Type {
		return -1, errors.New("type mismatch")
	}

	switch typ {
	case INT:
		col.Data = append(col.Data.([]int), value.(int))
		return len(col.Data.([]int)) - 1, nil
	case FLOAT:
		col.Data = append(col.Data.([]float64), value.(float64))
		return len(col.Data.([]float64)) - 1, nil
	case STRING:
		col.Data = append(col.Data.([]string), value.(string))
		return len(col.Data.([]string)) - 1, nil
	}

	return -1, nil
}

// GetRow returns the value in the given row.
func (col Column) GetRow(index int) (interface{}, error) {
	if index >= col.GetNumRows() || index < 0 {
		return nil, errors.New("index out of bounds")
	}

	switch col.Signature.Type {
	case INT:
		return (col.Data.([]int))[index], nil
	case FLOAT:
		return (col.Data.([]float64))[index], nil
	case STRING:
		return (col.Data.([]string))[index], nil
	}

	panic("invalid column signature")
}

// GetNumRows returns the number of rows present in the Column.
func (col Column) GetNumRows() int {
	switch col.Signature.Type {
	case INT:
		return len(col.Data.([]int))
	case FLOAT:
		return len(col.Data.([]float64))
	case STRING:
		return len(col.Data.([]string))
	}

	panic("invalid column signature")
}
