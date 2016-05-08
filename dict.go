package csgo
import "errors"

type DictEncodedDataStore struct {
    IS_LRE bool
    DataType DataTypes
    Dictionary map[int] interface {}
    Data DataStore
}

// GetDataType returns the type of the stored data.
func (ds DictEncodedDataStore) GetDataType() DataTypes {
    return ds.DataType
}

// AddRow adds a new row to the column.
func (ds *DictEncodedDataStore) AddRow(typ DataTypes, value interface{}) (int, error) {
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

    // TODO: XXX BEGIN XXX
    // looking up for matching value in the Hashtable
    index := -1
    for k, v := range ds.Dictionary {
        if v == value { // if found
            index := k  // hold on the key into index-variable
            break       // and break the look-up-loop
        }
    }

    if index == -1 { // in the case, no matching value found. That means index has not been changed
        index = len(ds.Dictionary)  // so generate a new key sequentially (keys-number + 1)
                                    // 1. case (Hashtable is empty) => key = keys-number (= 0) + 1 = 1
                                    // 2. case (Hashtable is not empty) => key = keys-number + 1
        ds.Dictionary[index] = value // add the (key, value) into the Hashtable
    }

    if ! ds.IS_LRE {
        // add the index of the value to the column (the index of the value not the value)
        ds.Data = append(ds.Data, index)
    } else {
        x, y := (RLEDataStore.(ds.Data)).AddRow(INT, index)
        return x, y
    }
    return /* len(ds.Data) */ ds.GetNumRows() - 1, nil
    // XXX END XXX
}

// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
func (ds *DictEncodedDataStore) GetRow(rowIndex int) (interface{}, error) {
    if rowIndex < ds.GetNumRows() {
        key := ds.Data[rowIndex]
        return ds.Dictionary[key], nil
    }
    return nil, errors.New("out of column's range")
}

// GetNumRows returns the number of rows currently included in this column
func (ds *DictEncodedDataStore) GetNumRows() int {
    if ! ds.IS_LRE {
        count := 0
        for i := range ds.Data {
            count++
        }
        return count //len(ds.Data)
    } else {
        return RLEDataStore.(ds).GetNumRows()
    }
}
