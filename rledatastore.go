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
	Entries  []RLEDataEntry
}

// NewRLEDataStore creates a new RLEDataStore
func NewRLEDataStore(dataType DataTypes) DataStore {
	return &RLEDataStore{dataType, []RLEDataEntry{}}
}

// GetDataType returns the type of the stored data.
func (ds RLEDataStore) GetDataType() DataTypes {
	return ds.DataType
}

// AddRow adds a new row to the column.
func (ds *RLEDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
	if typ != ds.DataType {
		return -1, errors.New("invalid type")
	}

	// check if value is of the right type
	wrongValue := false
	switch typ {
	case INT:
		wrongValue = value.(int)*0 == 1
	case FLOAT:
		wrongValue = value.(float64)*0.0 == 1.0
	case STRING:
		wrongValue = value.(string)+"+" == value
	}
	if wrongValue {
		panic("wrong data type")
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
