package main

import (
	"fmt"

	. "github.com/hoffmale/csgo"
)

func main() {
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
}
