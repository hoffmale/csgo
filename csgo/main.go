package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	. "github.com/hoffmale/csgo"
)

func test_case() {
	c := ColumnStore{Relations: make(map[string]Relationer)}

	c.CreateRelation("SalesJan2009", []AttrInfo{
		{Name: "Transaction_date", Type: STRING, Enc: NOCOMP},
		{Name: "Product", Type: STRING, Enc: NOCOMP},
		{Name: "Price", Type: INT, Enc: NOCOMP},
		{Name: "Payment_Type", Type: STRING, Enc: NOCOMP},
		{Name: "Name", Type: STRING, Enc: NOCOMP},
		{Name: "City", Type: STRING, Enc: NOCOMP},
		{Name: "State", Type: STRING, Enc: NOCOMP},
		{Name: "Country", Type: STRING, Enc: NOCOMP},
		{Name: "Account_Created", Type: STRING, Enc: NOCOMP},
		{Name: "Last_Login", Type: STRING, Enc: NOCOMP},
		{Name: "Latitude", Type: FLOAT, Enc: NOCOMP},
		{Name: "Longitude", Type: FLOAT, Enc: NOCOMP},
	})

	c.GetRelation("SalesJan2009").Load("SalesJan2009.csv", ',')
	fmt.Println("DEBUG: HIER 2")
	r := c.GetRelation("SalesJan2009").Select(AttrInfo{Name: "Country", Type: STRING, Enc: NOCOMP}, NEQ, "United States").Scan([]AttrInfo{
		{Name: "Name", Type: STRING, Enc: NOCOMP},
		{Name: "Latitude", Type: FLOAT, Enc: NOCOMP},
		{Name: "Longitude", Type: FLOAT, Enc: NOCOMP},
	})
	r.Print()

	c.CreateRelation("Sudent", []AttrInfo{
		{Name: "id", Type: INT, Enc: NOCOMP},
		{Name: "name", Type: STRING, Enc: NOCOMP},
	})

	c.CreateRelation("Result", []AttrInfo{
		{Name: "id", Type: INT, Enc: NOCOMP},
		{Name: "student_id", Type: INT, Enc: NOCOMP},
		{Name: "result", Type: FLOAT, Enc: NOCOMP},
	})

	for _, r := range c.Relations {
		r.Print()
	}
}

// to enable cpu profiling
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {

	//test_case()

	/*
		fmt.Println("This might become a useful console utility for our ColumnStore database")
		fmt.Println()
		// Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude
		salesRelation := Relation{Name: "SalesJan2009", Columns: []Column{
			{Signature: AttrInfo{Name: "Transaction_date", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Product", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Price", Type: INT, Enc: NOCOMP}, Data: make([]int, 0)},
			{Signature: AttrInfo{Name: "Payment_Type", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Name", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "City", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "State", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Country", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Account_Created", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Last_Login", Type: STRING, Enc: NOCOMP}, Data: make([]string, 0)},
			{Signature: AttrInfo{Name: "Latitude", Type: FLOAT, Enc: NOCOMP}, Data: make([]float64, 0)},
			{Signature: AttrInfo{Name: "Longitude", Type: FLOAT, Enc: NOCOMP}, Data: make([]float64, 0)},
		}}
		fmt.Println("loading values...")
		salesRelation.Load("SalesJan2009.csv", ',')
		fmt.Printf("loading done (%d rows loaded)", len(salesRelation.Columns[0].Data.([]string)))
		fmt.Println()
		salesRelation.Print()
		r := salesRelation.Select(AttrInfo{Name: "Country", Type: STRING, Enc: NOCOMP}, NEQ, "United States").Scan([]AttrInfo{
			{Name: "Name", Type: STRING, Enc: NOCOMP},
			{Name: "Latitude", Type: FLOAT, Enc: NOCOMP},
			{Name: "Longitude", Type: FLOAT, Enc: NOCOMP},
		})
		r.Print()
	*/

	// enable CPU profiling [BEGIN]
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	// enable CPU profiling [END]

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
	//tblPartSupp.Select(AttrInfo{"SUPPLYCOST", FLOAT, NOCOMP}, LT, float64(100.0)).Print()
}
