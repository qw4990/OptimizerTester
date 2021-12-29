package cost

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"

	"github.com/qw4990/OptimizerTester/tidb"
)

func genPointQueries(ins tidb.Instance, n int, sel, orderby, db, tbl string, cols ...string) Queries {
	rows := sampleCols(ins, n, db, tbl, cols...)
	queries := make(Queries, 0, n)
	for _, row := range rows {
		conds := make([]string, len(cols))
		for j, col := range cols {
			conds[j] = fmt.Sprintf("%v=%v", col, row[j])
		}
		queries = append(queries, Query{
			SQL:   fmt.Sprintf(`select %v from %v.%v where %v %v`, sel, db, tbl, strings.Join(conds, " and "), orderby),
			Label: "",
		})
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
				v := *(is[i].(*string))
				v = strings.Replace(v, "'", "\\'", -1)
				row[i] = fmt.Sprintf("'%v'", v)
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

func mustReadOneLine(ins tidb.Instance, q string, ret ...interface{}) {
	rs := ins.MustQuery(q)
	rs.Next()
	defer rs.Close()
	if err := rs.Scan(ret...); err != nil {
		panic(err)
	}
}

func mustGetRowCount(ins tidb.Instance, q string) int {
	var cnt int
	mustReadOneLine(ins, q, &cnt)
	return cnt
}

func randRange(minVal, maxVal, iter, totalRepeat int) (int, int) {
	step := (maxVal - minVal) / totalRepeat
	l := rand.Intn(step)
	r := rand.Intn(step) + step*(iter+1)
	if r > maxVal {
		r = maxVal
	}
	return l, r
}

func saveTo(f string, r interface{}) {
	data, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(f, data, 0666); err != nil {
		panic(err)
	}
}

func readFrom(f string, r interface{}) error {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, r); err != nil {
		return err
	}
	return nil
}

func readQueriesFrom(f string) (Queries, error) {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	var r Queries
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r, nil
}

func saveRecordsTo(r Records, f string) {
	data, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(f, data, 0666); err != nil {
		panic(err)
	}
}

func readRecordsFrom(f string) (Records, error) {
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, err
	}
	var r Records
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r, nil
}
