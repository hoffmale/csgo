package csgo

import "errors"

// RLEDataEntry is an entry in a run length encoded column.
type RLEDataEntry struct {
	Count int
	Value interface{}
}

// RLEDataStore is a run length encoded DataStore
type RLEDataStore struct {
	DataType DataTypes
	Flags    ColumnFlags
	Entries  []RLEDataEntry
}

// NewRLEDataStore creates a new RLEDataStore
func NewRLEDataStore(dataType DataTypes, flags ColumnFlags) DataStore {
	return &RLEDataStore{dataType, flags, []RLEDataEntry{}}
}

// GetDataType returns the type of the stored data.
func (ds RLEDataStore) GetDataType() DataTypes {
	return ds.DataType
}

// GetFlags returns the flags for the stored data
func (ds RLEDataStore) GetFlags() ColumnFlags {
	return ds.Flags
}

// AddRow adds a new row to the column.
func (ds *RLEDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
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

	if len(ds.Entries) > 0 {
		if ds.Entries[len(ds.Entries)-1].Value == value {
			ds.Entries[len(ds.Entries)-1].Count++
			return ds.GetNumRows() - 1, nil
		}
	}

	newEntry := RLEDataEntry{1, value}
	ds.Entries = append(ds.Entries, newEntry)
	return ds.GetNumRows() - 1, nil
}

// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
func (ds RLEDataStore) GetRow(rowIndex int) (interface{}, error) {
	entryCount := 0
	for _, entry := range ds.Entries {
		entryCount += entry.Count
		if entryCount > rowIndex {
			return entry.Value, nil
		}
	}

	return nil, errors.New("value not found")
}

// GetNumRows returns the number of rows currently included in this column
func (ds RLEDataStore) GetNumRows() int {
	entryCount := 0
	for _, entry := range ds.Entries {
		entryCount += entry.Count
	}
	return entryCount
}
