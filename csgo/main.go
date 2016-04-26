package main

import (
	"fmt"

	. "github.com/hoffmale/csgo"
)

func main() {
	fmt.Print("This might become a useful console utility for our ColumnStore database")
	//
	r := Relation{Name: "keep import!", Columns: []Column{}}
	if r.Name == "import" {
		fmt.Print("this should never happen")
	}
}
