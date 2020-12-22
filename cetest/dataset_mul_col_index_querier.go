package cetest

type mulColIndexQuerier struct {
	db              string
	tbs             []string   // table names
	cols            [][]string // table columns' names
	colTypes        [][]DATATYPE
	orderedDistVals [][][]string // ordered distinct values
	valActRows      [][][]int    // actual row count
}
