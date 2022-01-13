package querygen

import "github.com/pingcap/tidb/parser/types"

type table struct {
	Name           string
	Cols           []*column
	singleColIdxes []*index
	multiColIdxes  []*index
	RowCount       uint
}

type column struct {
	Name     string
	TP       types.FieldType
	NDV      uint
	Max      string
	Min      string
	RandVals []string
}

func findColByName(cols []*column, name string) *column {
	for _, col := range cols {
		if col.Name == name {
			return col
		}
	}
	return nil
}

type index struct {
	Name string
	Cols []*column
}
