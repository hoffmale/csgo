package csgo

import "errors"

// BasicDataStore is a basic, uncompressed DataStore
type BasicDataStore struct {
	// DataType represents the type of the stored data
	DataType DataTypes
	// Flags are the ColumnFlags for the stored data
	Flags ColumnFlags
	// Values is a slice of type DataType. It contains the data of this column.
	Values []interface{}
}

// NewBasicDataStore create a new BasicDataStore
func NewBasicDataStore(dataType DataTypes, flags ColumnFlags) *BasicDataStore {
	ds := BasicDataStore{DataType: dataType, Flags: flags, Values: []interface{}{}}
	return &ds
}

// GetDataType returns the type of the values in the DataStore
func (ds BasicDataStore) GetDataType() DataTypes {
	return ds.DataType
}

// GetFlags returns the flags for the values in the DataStore
func (ds BasicDataStore) GetFlags() ColumnFlags {
	return ds.Flags
}

// AddRow adds a row to the DataStore.
func (ds *BasicDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
	if typ != ds.DataType {
		return -1, errors.New("invalid data type")
	}

	// check if value is of the right type
	rightType := false

	switch {
	case ds.Flags == 0:
		switch typ {
		case INT:
			_, rightType = value.(int)
		case FLOAT:
			_, rightType = value.(float64)
		case STRING:
			_, rightType = value.(string)
		}
	case ds.Flags&GROUPED == GROUPED && ds.Flags&NULLABLE == 0:
		switch typ {
		case INT:
			_, rightType = value.([]int)
		case FLOAT:
			_, rightType = value.([]float64)
		case STRING:
			_, rightType = value.([]string)
		}
	}

	if !rightType {
		return -1, errors.New("type mismatch")
	}

	// add value
	switch {
	case ds.Flags == 0:
		ds.Values = append(ds.Values, value)
	case ds.Flags&GROUPED != 0 && ds.Flags&NULLABLE == 0:
		ds.Values = append(ds.Values, value)
	}
	return ds.GetNumRows() - 1, nil
}

// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
func (ds BasicDataStore) GetRow(rowIndex int) (interface{}, error) {
	if rowIndex < 0 || rowIndex >= ds.GetNumRows() {
		return nil, errors.New("index out of bounds")
	}
	switch {
	case ds.Flags == 0:
		return ds.Values[rowIndex], nil
	case ds.Flags&GROUPED != 0 && ds.Flags&NULLABLE == 0:
		return ds.Values[rowIndex], nil
	}
	return nil, errors.New("unknown type")
}

// GetNumRows returns the number of rows currently included in this column
func (ds BasicDataStore) GetNumRows() int {
	return len(ds.Values)
}
