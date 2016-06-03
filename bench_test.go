package csgo

import "testing"

func BenchmarkSimpleSelect(b *testing.B) {
	cs := ColumnStore{}
	tblPartSupp := cs.CreateRelation("PARTSUPP", []AttrInfo{
		{Name: "PARTKEY", Type: INT, Enc: NOCOMP},
		{Name: "SUPPKEY", Type: INT, Enc: NOCOMP},
		{Name: "AVAILQTY", Type: INT, Enc: NOCOMP},
		{Name: "SUPPLYCOST", Type: FLOAT, Enc: NOCOMP},
		{Name: "COMMENT", Type: STRING, Enc: NOCOMP},
	})
	tblPartSupp.Load("partsupp.tbl", '|')

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tblPartSupp.Select(AttrInfo{Name: "SUPPLYCOST", Type: FLOAT, Enc: NOCOMP}, LT, float64(0.0))
	}
}

func BenchmarkComplexSelect(b *testing.B) {
	cs := ColumnStore{}
	tblPartSupp := cs.CreateRelation("PARTSUPP", []AttrInfo{
		{Name: "PARTKEY", Type: INT, Enc: NOCOMP},
		{Name: "SUPPKEY", Type: INT, Enc: NOCOMP},
		{Name: "AVAILQTY", Type: INT, Enc: NOCOMP},
		{Name: "SUPPLYCOST", Type: FLOAT, Enc: NOCOMP},
		{Name: "COMMENT", Type: STRING, Enc: NOCOMP},
	})
	tblSupplier := cs.CreateRelation("SUPPLIER", []AttrInfo{
		{"SUPPKEY", INT, NOCOMP, 0},
		{"NAME", STRING, NOCOMP, 0},
		{"ADDRESS", STRING, NOCOMP, 0},
		{"NATIONKEY", INT, NOCOMP, 0},
		{"PHONE", STRING, NOCOMP, 0},
		{"ACCTBAL", FLOAT, NOCOMP, 0},
		{"COMMENT", STRING, NOCOMP, 0},
	})
	tblPart := cs.CreateRelation("PART", []AttrInfo{
		{"PARTKEY", INT, NOCOMP, 0},
		{"NAME", STRING, NOCOMP, 0},
		{"MFGR", STRING, NOCOMP, 0},
		{"BRAND", STRING, NOCOMP, 0},
		{"TYPE", STRING, NOCOMP, 0},
		{"SIZE", INT, NOCOMP, 0},
		{"CONTAINER", STRING, NOCOMP, 0},
		{"RETAILPRICE", FLOAT, NOCOMP, 0},
		{"COMMENT", STRING, NOCOMP, 0},
	})

	tblPartSupp.Load("partsupp.tbl", '|')
	tblSupplier.Load("supplier.tbl", '|')
	tblPart.Load("part.tbl", '|')

	b.ResetTimer()

	for benchLoop := 0; benchLoop < b.N; benchLoop++ {

		negativeSuppliers := tblSupplier.Scan([]AttrInfo{{"SUPPKEY", INT, NOCOMP, 0}, {"ACCTBAL", FLOAT, NOCOMP, 0}}).Select(AttrInfo{"ACCTBAL", FLOAT, NOCOMP, 0}, LT, float64(0.0))
		//negativeSuppliers.Print()

		for i := 0; i < 10; i++ {
			suppKey, _ := (negativeSuppliers.(Relation)).Columns[0].GetRow(i)

			suppliedParts := tblPartSupp.Scan([]AttrInfo{{"PARTKEY", INT, NOCOMP, 0}, {"SUPPKEY", INT, NOCOMP, 0}}).Select(AttrInfo{"SUPPKEY", INT, NOCOMP, 0}, EQ, suppKey).(Relation)
			for j := 0; j < suppliedParts.Columns[0].GetNumRows(); j++ {
				partKey, _ := suppliedParts.Columns[0].GetRow(j)
				tblPart.Select(AttrInfo{"PARTKEY", INT, NOCOMP, 0}, EQ, partKey) //.Print()
			}
		}
	}
}
