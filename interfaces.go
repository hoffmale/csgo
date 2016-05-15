// Package csgo contains an interface for implementing an In-Memory Column
// Store in pure Go for teaching purposes.
package csgo

// TODO: Session 1 - Implement the Relationer and ColumnStorer interface by
// using e.g. the Relation and ColumnStore struct (i.e. all method signatures/
// heads in a separte file). Implement Load, Scan, Select, Print, GetRawData,
// CreateRelation and GetRelation.
// TODO: Session 2 - Implement HashJoin and Aggregate
// TODO: Session 3 - Parallisation and Acceleration

// Comparison is an enum type for all possible comparison operations used e.g.
// for Select predicates.
type Comparison string

const (
	// EQ is the "equality" comparison operation.
	EQ Comparison = "=="
	// NEQ is the "negative equality" comparison operation.
	NEQ Comparison = "!="
	// LT is the "lesser than" comparison operation.
	LT Comparison = "<"
	// LEQ is the "lesser equal than" comparison operation.
	LEQ Comparison = "<="
	// GT is the "greater than" comparison operation.
	GT Comparison = ">"
	// GEQ is the "greater equal than" comparison operation.
	GEQ Comparison = ">="
)

// Compression is an enumeration type for all supported column encoding methods.
type Compression int

const (
	// NOCOMP means that no encoding method is used.
	NOCOMP Compression = iota
	// RLE is the run-length encoding method.
	RLE
	// DICT is the dictionary encoding method.
	DICT
	// FOR is the frame of reference encoding method.
	FOR
)

// JoinType defines all supported types of join.
type JoinType int

const (
	// EQUI is an inner join allowing only equality comparisons between columns.
	EQUI JoinType = iota
	// SEMI behaves like a natural join, but only projects the reduction of the
	// left relation.
	SEMI
	// LEFTOUTER returns all records of the left relation and possibly matching
	// records of the right relation.
	LEFTOUTER
	// RIGHTOUTER returns all records of the right relation and possibly matching
	// records of the left relation.
	RIGHTOUTER
)

// AggrFunc is an enumeration type for all predefined functions of aggregation.
type AggrFunc int

const (
	// COUNT retuns the number of all elements of a collection.
	COUNT AggrFunc = iota
	// SUM returns the sum of values for all elements of a collection.
	SUM
	// MIN returns the lowest value for all elements of a collection.
	MIN
	// MAX returns the highest value for all elements of a collection.
	MAX
)

// DataTypes is the enumeration of all supported column data types
type DataTypes int

const (
	// INT represents the integer data type
	INT DataTypes = iota
	// FLOAT represents the decimal data type
	FLOAT
	// STRING represents the character string data type
	STRING
)

// AttrInfo contains meta information about a column (name and type).
type AttrInfo struct {
	// Name is the name of the column.
	Name string
	// Type is the type of the column (int, float or string).
	Type DataTypes
	// Enc defines the encoding of this column.
	Enc Compression
}

// Column is a single column containing the signature and the payload.
type Column struct {
	// Signature gives meta information about the column.
	Signature AttrInfo
	// Data contains the raw or compressed data (e.g. in the form of a slice).
	Data interface{}
}

// Relation is an example structure on which one could define the Relationer
// methods.
type Relation struct {
	// Name is the name of the relation as string representation.
	Name string
	// Columns is the collection of all columns of this relation.
	Columns []Column
}

// Relationer is an interface for a table/relation within a ColumnStore.
type Relationer interface {
	// Load should load and insert the data of a CSV file into the column store.
	// csvFile is the path to the CSV File.
	// separator is separator character used in the file.
	Load(csvFile string, separator rune)

	// Scan should simply return the specified columns of the relation.
	Scan(colList []AttrInfo) Relationer

	// Select should return a filtered collection of records defined by predicate
	// arguments (col, comp, compVal) of one relation.
	// col represents the column used for comparison.
	// comp defines the type of comparison.
	// compVal is the value used for the comparison.
	Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer

	// Print should output the relation to the standard output in record
	// representation.
	Print()

	// GetRawData should return all columns as a slice of slices (columns) with
	// the underlying type (int, float, string) in decompressed form and the
	// corresponding meta information.
	GetRawData() ([]interface{}, []AttrInfo)

	// HashJoin should implement the hash join operator between two relations.
	// rightRelation is the right relation for the hash join
	// joinType specifies the kind of hash join (inner, outer, semi ...)
	// compType specifies the comparison type for the join.
	// The join may be executed on one or more columns of each relation.
	HashJoin(col1 []AttrInfo, rightRelation Relationer, col2 []AttrInfo, joinType JoinType, compType Comparison) Relationer

	// Aggregate should implement the grouping and aggregation of columns.
	// groupBy specifies on which columns it should be grouped.
	// aggregate defines the column on which the aggrFunc should be applied.
	Aggregate(groupBy []AttrInfo, aggregate AttrInfo, aggrFunc AggrFunc) Relationer
}

// ColumnStore is an example structure on which one could define the
// ColumnStorer methods.
type ColumnStore struct {
	// Relations is the mapping of relation names to their object reference.
	Relations map[string]Relationer
}

// ColumnStorer is an interface for an In-Memory Column Store (the database).
type ColumnStorer interface {
	// CreateRelation creates a new relation within the column store and returns
	// an object reference.
	CreateRelation(tabName string, sig []AttrInfo) Relationer
	// GetRelation returns the object reference of a relation associated with the
	// passed relation name.
	GetRelation(relName string) Relationer
}
