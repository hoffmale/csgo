package main

import "github.com/hoffmale/csgo"

func main() {
    r1 := Relation{Name: "R1", Columns: []Column{
        NewColumnWithData(AttrInfo{Name: "v0", Type: INT, Enc: NOCOMP}, []int{27, 18, 28, 18, 28}),
        NewColumnWithData(AttrInfo{Name: "k", Type: STRING, Enc: NOCOMP}, []string{"Johna", "Alan", "Glory", "Popeye", "Alan"}),
	}}

    r2 := Relation{Name: "R1", Columns: []Column{
		NewColumnWithData(AttrInfo{Name: "k", Type: STRING, Enc: NOCOMP}, []string{"Johna", "Johna", "Alan", "Alan", "Glory"}),
		NewColumnWithData(AttrInfo{Name: "v1", Type: STRING, Enc: NOCOMP}, []string{"Whale", "Spider", "Ghosts", "Zombies", "Buffy"}),
	}}

    r1col, r1sig := r1.GetRawData()
    r2col, r2sig := r2.GetRawData()

    r3 := r2.HashJoin(r1sig, r2, r2sig, EQUI, NOCOMP)

    r3.Print()
}
