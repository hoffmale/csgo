package csgo

// CreateRelation creates a new relation within the column store and returns
// an object reference.
func (c ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	/*
		Relation: Relation = {
			- Name: string
			- Columns: Column[] = [
					- Signature: AttrInfo = {
							- Name: string
							- Type: DataTypes
							- Enc: Compression
						}
					- Data: slice
				]
		}
	*/
	// Relations is the mapping of relation names to their object reference.
	// Relations map[string]Relationer
	if (c.Relations == nil) {
		c.Relations = make(map[string]Relationer)
	}

	r, exist := c.Relations[tabName]
	if (!exist) {
		var rel Relation
		rel.Name = tabName
		rel.Columns = make([]Column, len(sig), len(sig))

		for index := 0; index < len(sig); index++ {
			rel.Columns[index].Signature = sig[index]
		}

		return rel
	}

	return r // die Alte vorhandene Relation, die existiert ist
}

// GetRelation returns the object reference of a relation associated with the
// passed relation name.
func (c ColumnStore) GetRelation(relName string) Relationer {
	return nil
}
