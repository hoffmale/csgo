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
	tblPartSupp.Load("D:\\Downloads\\table\\partsupp.tbl", '|')

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
		{"SUPPKEY", INT, NOCOMP},
		{"NAME", STRING, NOCOMP},
		{"ADDRESS", STRING, NOCOMP},
		{"NATIONKEY", INT, NOCOMP},
		{"PHONE", STRING, NOCOMP},
		{"ACCTBAL", FLOAT, NOCOMP},
		{"COMMENT", STRING, NOCOMP},
	})
	tblPart := cs.CreateRelation("PART", []AttrInfo{
		{"PARTKEY", INT, NOCOMP},
		{"NAME", STRING, NOCOMP},
		{"MFGR", STRING, NOCOMP},
		{"BRAND", STRING, NOCOMP},
		{"TYPE", STRING, NOCOMP},
		{"SIZE", INT, NOCOMP},
		{"CONTAINER", STRING, NOCOMP},
		{"RETAILPRICE", FLOAT, NOCOMP},
		{"COMMENT", STRING, NOCOMP},
	})

	tblPartSupp.Load("partsupp.tbl", '|')
	tblSupplier.Load("supplier.tbl", '|')
	tblPart.Load("part.tbl", '|')

	b.ResetTimer()

	for benchLoop := 0; benchLoop < b.N; benchLoop++ {

		negativeSuppliers := tblSupplier.Scan([]AttrInfo{{"SUPPKEY", INT, NOCOMP}, {"ACCTBAL", FLOAT, NOCOMP}}).Select(AttrInfo{"ACCTBAL", FLOAT, NOCOMP}, LT, float64(0.0))
		//negativeSuppliers.Print()

		for i := 0; i < 10; i++ {
			suppKey, _ := (negativeSuppliers.(Relation)).Columns[0].GetRow(i)

			suppliedParts := tblPartSupp.Scan([]AttrInfo{{"PARTKEY", INT, NOCOMP}, {"SUPPKEY", INT, NOCOMP}}).Select(AttrInfo{"SUPPKEY", INT, NOCOMP}, EQ, suppKey).(Relation)
			for j := 0; j < suppliedParts.Columns[0].GetNumRows(); j++ {
				partKey, _ := suppliedParts.Columns[0].GetRow(j)
				tblPart.Select(AttrInfo{"PARTKEY", INT, NOCOMP}, EQ, partKey) //.Print()
			}
		}
	}
}
