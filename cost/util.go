package cost

import (
	"fmt"
	"strings"

	"github.com/qw4990/OptimizerTester/tidb"
)

func genPointQueries(ins tidb.Instance, n int, sel, orderby, db, tbl string, cols ...string) []string {
	rows := sampleCols(ins, n, db, tbl, cols...)
	queries := make([]string, n)
	for i, row := range rows {
		conds := make([]string, len(cols))
		for j, col := range cols {
			conds[j] = fmt.Sprintf("%v=%v", col, row[j])
		}
		queries[i] = fmt.Sprintf(`select %v from %v.%v where %v %v`, sel, db, tbl, strings.Join(conds, "and"), orderby)
	}
	return queries
}

func sampleCols(ins tidb.Instance, n int, db, tbl string, cols ...string) [][]string {
	ins.MustExec(fmt.Sprintf("use %v", db))
	cs := strings.Join(cols, ", ")
	m := n * 256 // don't use order by rand() or distinct to avoid OOM
	q := fmt.Sprintf(`select %v from %v.%v limit %v`, cs, db, tbl, m)
	r := ins.MustQuery(q)
	ts, err := r.ColumnTypes()
	if err != nil {
		panic(err)
	}

	rows := make([][]string, 0, m)
	for r.Next() {
		is := make([]interface{}, len(ts))
		for i, t := range ts {
			switch t.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "NVARCHAR":
				is[i] = new(string)
			case "INT", "BIGINT":
				is[i] = new(int)
			case "DECIMAL":
				is[i] = new(float64)
			default:
				panic(fmt.Sprintf("unknown database type name %v", t.DatabaseTypeName()))
			}
		}

		if err := r.Scan(is...); err != nil {
			panic(err)
		}
		row := make([]string, len(ts))
		for i, t := range ts {
			switch t.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "NVARCHAR":
				row[i] = fmt.Sprintf("'%v'", *(is[i].(*string)))
			case "INT", "BIGINT":
				row[i] = fmt.Sprintf("%v", *(is[i].(*int)))
			case "DECIMAL":
				row[i] = fmt.Sprintf("%v", *(is[i].(*float64)))
			}
		}
		rows = append(rows, row)
	}

	// select n rows randomly
	results := make([][]string, 0, n)
	dup := make(map[string]struct{})
	for _, row := range rows {
		key := strings.Join(row, ":")
		if _, ok := dup[key]; ok {
			continue
		}
		dup[key] = struct{}{}
		results = append(results, row)
		if len(results) == n {
			break
		}
	}

	return results
}
