package csgo

// CreateRelation creates a new relation within the column store and returns
// an object reference.
func (c ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	return nil
}

// GetRelation returns the object reference of a relation associated with the
// passed relation name.
func (c ColumnStore) GetRelation(relName string) Relationer {
	return nil
}
