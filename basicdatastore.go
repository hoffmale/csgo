package csgo

import "errors"

// BasicDataStore is a basic, uncompressed DataStore
type BasicDataStore struct {
	// DataType represents the type of the stored data
	DataType DataTypes
	// Values is a slice of type DataType. It contains the data of this column.
	Values interface{}
}

// NewBasicDataStore create a new BasicDataStore
func NewBasicDataStore(dataType DataTypes) *BasicDataStore {
	ds := BasicDataStore{DataType: dataType}
	switch dataType {
	case INT:
		ds.Values = []int{}
	case FLOAT:
		ds.Values = []float64{}
	case STRING:
		ds.Values = []string{}
	}
	return &ds
}

// GetDataType returns the type of the values in the DataStore
func (ds BasicDataStore) GetDataType() DataTypes {
	return ds.DataType
}

// AddRow adds a row to the DataStore.
func (ds *BasicDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
	if typ != ds.DataType {
		return -1, errors.New("invalid data type")
	}
	switch typ {
	case INT:
		ds.Values = append(ds.Values.([]int), value.(int))
	case FLOAT:
		ds.Values = append(ds.Values.([]float64), value.(float64))
	case STRING:
		ds.Values = append(ds.Values.([]string), value.(string))
	}
	return ds.GetNumRows() - 1, nil
}

// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
func (ds BasicDataStore) GetRow(rowIndex int) (interface{}, error) {
	if rowIndex < 0 || rowIndex >= ds.GetNumRows() {
		return nil, errors.New("index out of bounds")
	}
	switch ds.DataType {
	case INT:
		return (ds.Values.([]int))[rowIndex], nil
	case FLOAT:
		return (ds.Values.([]float64))[rowIndex], nil
	case STRING:
		return (ds.Values.([]string))[rowIndex], nil
	}
	return nil, errors.New("unknown type")
}

// GetNumRows returns the number of rows currently included in this column
func (ds BasicDataStore) GetNumRows() int {
	switch ds.DataType {
	case INT:
		return len(ds.Values.([]int))
	case FLOAT:
		return len(ds.Values.([]float64))
	case STRING:
		return len(ds.Values.([]string))
	}
	return -1
}
