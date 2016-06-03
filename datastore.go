package csgo

// DataStore is an internal interface for storing column data
type DataStore interface {
	// GetDataType returns the type of the stored data.
	GetDataType() DataTypes
	// GetFlags returns the flags for the stored data
	GetFlags() ColumnFlags
	// AddRow adds a new row to the column.
	AddRow(typ DataTypes, value interface{}) (int, error)
	// GetRow returns the value at the indicated row. If that value can not be found, an error is returned.
	GetRow(rowIndex int) (interface{}, error)
	// GetNumRows returns the number of rows currently included in this column
	GetNumRows() int
}
