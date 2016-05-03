package csgo

// CreateRelation creates a new relation within the column store and returns
// an object reference.
func (c ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	if c.Relations == nil {
		c.Relations = make(map[string]Relationer)
	}

	r, exist := c.Relations[tabName]

	if !exist && len(sig) > 0 {
		relation := Relation{Name: tabName, Columns: []Column{}}
		// somehow, the following lines bugged:
		//var relation Relation
		//relation.Name = tabName
		//relation.Columns = make([]Column, len(sig), len(sig))

		for _, colSig := range sig {
			relation.Columns = append(relation.Columns, NewColumn(colSig))
		}
		c.Relations[tabName] = relation
		return relation
	}

	return r // die Alte vorhandene Relation, die existiert ist
}

// GetRelation returns the object reference of a relation associated with the
// passed relation name.
func (c ColumnStore) GetRelation(relName string) Relationer {
	if c.Relations == nil {
		return nil
	}
	relation, exist := c.Relations[relName]
	if exist {
		return relation
	}

	return nil
}
