package csgo

// Load should load and insert the data of a CSV file into the column store.
// csvFile is the path to the CSV File.
// separator is separator character used in the file.
func (r Relation) Load(csvFile string, separator rune) {}

// Scan should simply return the specified columns of the relation.
func (r Relation) Scan(colList []AttrInfo) Relationer {
	return nil
}

// Select should return a filtered collection of records defined by predicate
// arguments (col, comp, compVal) of one relation.
// col represents the column used for comparison.
// comp defines the type of comparison.
// compVal is the value used for the comparison.
func (r Relation) Select(col AttrInfo, comp Comparison, compVal interface{}) Relationer {
	return nil
}

// Print should output the relation to the standard output in record
// representation.
func (r Relation) Print() {}

// GetRawData should return all columns as a slice of slices (columns) with
// the underlying type (int, float, string) in decompressed form and the
// corresponding meta information.
func (r Relation) GetRawData() ([]interface{}, []AttrInfo) {
	return nil, nil
}

// HashJoin should implement the hash join operator between two relations.
// joinType specifies the kind of hash join (inner, outer, semi ...)
// The join may be executed on one or more columns of each relation.
// currently not implemented
func (r Relation) HashJoin(col1 []AttrInfo, input2 []Column, col2 []AttrInfo, joinType JoinType) Relationer {
	return nil
}

// Aggregate should implement the grouping and aggregation of columns.
// groupBy specifies on which columns it should be grouped.
// aggregate defines the column on which the aggrFunc should be applied.
// currently not implemented
func (r Relation) Aggregate(groupBy []AttrInfo, aggregate AttrInfo, aggrFunc AggrFunc) Relationer {
	return nil
}
