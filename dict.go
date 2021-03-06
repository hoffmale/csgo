package csgo

import "errors"

// DictEncodedDataStore is a DataStore apllying dictionary encoding
type DictEncodedDataStore struct {
	DataType   DataTypes
	Flags      ColumnFlags
	Dictionary map[int]interface{}

	Data DataStore
}

// NewDictEncodedDataStore creates a new DictEncodedDataStore.
func NewDictEncodedDataStore(typ DataTypes, flags ColumnFlags, internalDataStoreType Compression) DataStore {
	ds := &DictEncodedDataStore{DataType: typ, Flags: flags, Dictionary: map[int]interface{}{}}
	switch internalDataStoreType {
	case NOCOMP:
		ds.Data = NewBasicDataStore(INT, 0)
	case RLE:
		ds.Data = NewRLEDataStore(INT, 0)
	case DICT:
		// prevent unnecessary looping
		ds.Data = NewBasicDataStore(INT, 0)
	default:
		ds.Data = NewBasicDataStore(INT, 0)
	}
	return ds
}

// GetDataType returns the type of the stored data.
func (ds *DictEncodedDataStore) GetDataType() DataTypes {
	return ds.DataType
}

// GetFlags returns the flags for the stored data
func (ds DictEncodedDataStore) GetFlags() ColumnFlags {
	return ds.Flags
}

// AddRow adds a new row to the column.
func (ds *DictEncodedDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
	if typ != ds.DataType {
		return -1, errors.New("invalid type")
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

	// looking up for matching value in the Hashtable
	index := -1
	for k, v := range ds.Dictionary {
		if v == value { // if found
			index = k // hold on the key into index-variable
			break     // and break the look-up-loop
		}
	}

	if index == -1 { // in the case, no matching value found. That means index has not been changed
		index = len(ds.Dictionary) // so generate a new key sequentially (keys-number + 1)
		// 1. case (Hashtable is empty) => key = keys-number (= 0) + 1 = 1
		// 2. case (Hashtable is not empty) => key = keys-number + 1
		ds.Dictionary[index] = value // add the (key, value) into the Hashtable
	}

	// add the index of the value to the column (the index of the value not the value)
	ds.Data.AddRow(INT, index)           // = append(Data, index)
	return ds.Data.GetNumRows() - 1, nil //len(Data) - 1, nil
}

// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
func (ds *DictEncodedDataStore) GetRow(rowIndex int) (interface{}, error) {
	if rowIndex < ds.Data.GetNumRows() /*len(Data)*/ && rowIndex >= 0 {
		key, err := ds.Data.GetRow(rowIndex) //ds.Data[rowIndex]
		if err != nil {
			return nil, err
		}
		return ds.Dictionary[key.(int)], nil
	}
	return nil, errors.New("out of column's range")
}

// GetNumRows returns the number of rows currently included in this column
func (ds *DictEncodedDataStore) GetNumRows() int {
	return ds.Data.GetNumRows() //len(ds.Data)
}
