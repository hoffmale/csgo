package csgo

// CreateRelation creates a new relation within the column store and returns
// an object reference.
func (c ColumnStore) CreateRelation(tabName string, sig []AttrInfo) Relationer {
	if (c.Relations == nil) {
		c.Relations = make(map[string]Relationer)
	}

	r, exist := c.Relations[tabName]

	if (!exist) {
		var relation Relation
		relation.Name = tabName
		relation.Columns = make([]Column, len(sig), len(sig))

		for index := 0; index < len(sig); index++ {
			relation.Columns[index].Signature = sig[index]
			if (sig[index].Type == INT) {
				relation.Columns[index].Data = make([]int, 0)
			} else {
				if (sig[index].Type == FLOAT) {
					relation.Columns[index].Data = make([]float64, 0)
				} else {
				relation.Columns[index].Data = make([]string, 0)
				}
			}
		}
		c.Relations[tabName] = relation
		return relation
	}

	return r // die Alte vorhandene Relation, die existiert ist
}

// GetRelation returns the object reference of a relation associated with the
// passed relation name.
func (c ColumnStore) GetRelation(relName string) Relationer {
	if (c.Relations == nil) {
		return nil
	} else {
		relation, exist := c.Relations[relName]
		if (exist) {
			return relation
		}
	}

	return nil
}
