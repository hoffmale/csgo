package csgo

import "testing"

func TestColumnStoreCreateRelation(t *testing.T) {
	c := ColumnStore{Relations: make(map[string]Relationer)}

	r := c.CreateRelation("testRel", []AttrInfo{})
	if r != nil {
		t.Error("Empty relation got created")
		t.Fail()
	}

	r = c.CreateRelation("testRel2", []AttrInfo{{Name: "testCol1", Type: INT, Enc: NOCOMP}})
	if r == nil {
		t.Error("No relation got created (creation was expected)")
		t.Fail()
	}

	c = ColumnStore{Relations: nil}
	r = c.CreateRelation("testRel3", []AttrInfo{{Name: "testCol2", Type: FLOAT, Enc: NOCOMP}})
	if r == nil {
		t.Error("ColumnStore didnt auto-create Relations map")
		t.Fail()
	}
}

func TestColumnStoreGetRelation(t *testing.T) {
	c := ColumnStore{Relations: make(map[string]Relationer)}

	r := c.GetRelation("testRel1")
	if r != nil {
		t.Error("Got a relation from empty column store")
		t.Log(r)
		t.Fail()
	}

	c.CreateRelation("testRel2", []AttrInfo{{Name: "testCol1", Type: INT, Enc: NOCOMP}})
	r = c.GetRelation("testRel2")
	if r == nil {
		t.Error("Got no relation although corresponding relation was just created")
		t.Fail()
	}

	c = ColumnStore{Relations: nil}
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("error in column store with Relations=nil: %#v", r)
			t.Fail()
		}
	}()
	r = c.GetRelation("testRel3")
	if r != nil {
		t.Error("Got a relation from uninitialised column store")
		t.Fail()
	}
}
