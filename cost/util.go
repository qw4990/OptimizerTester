package cost

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

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
	step := (maxVal - minVal + 1) / totalRepeat
	l := 1
	r := step * (iter + 1)
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

var costFactorVars = []string{"tidb_opt_cpu_factor",
	"tidb_opt_copcpu_factor", "tidb_opt_network_factor",
	"tidb_opt_scan_factor", "tidb_opt_desc_factor",
	"tidb_opt_memory_factor", "tidb_opt_seek_factor"}

func setCostFactors(ins tidb.Instance, factors CostFactors) {
	fmt.Println("SET COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek):", factors)
	for i := 0; i < NumFactors; i++ {
		sql := fmt.Sprintf("set @@%v=%v;", costFactorVars[i], factors[i])
		fmt.Println(sql)
		ins.MustExec(sql)
	}
}

func readCostFactors(ins tidb.Instance) (factors CostFactors) {
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	for i := 0; i < NumFactors; i++ {
		ret := ins.MustQuery(fmt.Sprintf("select @@%v", costFactorVars[i]))
		ret.Next()
		if err := ret.Scan(&factors[i]); err != nil {
			panic(err)
		}
		if err := ret.Close(); err != nil {
			panic(err)
		}
	}
	fmt.Println("READ COST FACTORS(CPU, CopCPU, Net, Scan, DescScan, Mem, Seek):", factors)
	return
}

func calculateCost(weights CostWeights, factors CostFactors) float64 {
	var cost float64
	for i := range factors {
		cost += weights[i] * factors[i]
	}
	return cost
}

func extractCostTimeFromQuery(ins tidb.Instance, query string, repeat int, checkRowCount bool) (avgPlanCost, avgTimeMS float64) {
	query = "explain analyze " + query
	var totalPlanCost, totalTimeMS float64
	for i := 0; i < repeat+1; i++ {
		rs := ins.MustQuery(query)
		planCost, timeMS := extractCostTime(rs, query, checkRowCount)
		fmt.Printf("[cost-eval/cali] iter: %v, cost: %v, timeMS: %v, query: %v\n", i, planCost, timeMS, query)
		if i == 0 {
			continue // ignore the first processing
		}
		totalPlanCost += planCost
		totalTimeMS += timeMS
	}
	return totalPlanCost / float64(repeat), totalTimeMS / float64(repeat)
}

func extractCostTime(explainAnalyzeResults *sql.Rows, q string, checkRowCount bool) (planCost, timeMS float64) {
	//mysql> explain analyze select /*+ use_index(t, b) */ * from synthetic.t where b>=1 and b<=100000;
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| id                            | estRows   | estCost     | actRows | task      | access object       | execution info                                                                                                                                                                                                                                                 | operator info                      | memory  | disk |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	//	| IndexLookUp_7                 | 100109.36 | 11149394.98 | 99986   | root      |                     | time:252.5ms, loops:99, index_task: {total_time: 134.2ms, fetch_handle: 98.2ms, build: 7.86µs, wait: 36ms}, table_task: {total_time: 666.2ms, num: 9, concurrency: 5}                                                                                          |                                    | 37.7 MB | N/A  |
	//	| ├─IndexRangeScan_5(Build)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t, index:b(b) | time:93.2ms, loops:102, cop_task: {num: 1, max: 89.6ms, proc_keys: 0, tot_proc: 89ms, rpc_num: 1, rpc_time: 89.6ms, copr_cache_hit_ratio: 0.00}, tikv_task:{time:59.4ms, loops:99986}                                                                          | range:[1,100000], keep order:false | N/A     | N/A  |
	//	| └─TableRowIDScan_6(Probe)     | 100109.36 | 5706253.48  | 99986   | cop[tikv] | table:t             | time:592.1ms, loops:109, cop_task: {num: 9, max: 89.2ms, min: 10.4ms, avg: 54.1ms, p95: 89.2ms, tot_proc: 456ms, rpc_num: 9, rpc_time: 486.3ms, copr_cache_hit_ratio: 0.00}, tikv_task:{proc max:15ms, min:2.57ms, p80:10.9ms, p95:15ms, iters:99986, tasks:9} | keep order:false                   | N/A     | N/A  |
	//	+-------------------------------+-----------+-------------+---------+-----------+---------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------+---------+------+
	rs := explainAnalyzeResults
	var id, task, access, execInfo, opInfo, mem, disk, rootExecInfo string
	var estRows, actRows, cost float64
	planLabel := "Unmatched"

	for rs.Next() {
		if err := rs.Scan(&id, &estRows, &cost, &actRows, &task, &access, &execInfo, &opInfo, &mem, &disk); err != nil {
			panic(err)
		}
		if checkRowCount && actRows != estRows {
			//fmt.Printf("[cost-eval] worker-%v not true-CE for query=%v, est=%v, act=%v\n", id, q, estRows, actRows)
			panic(fmt.Sprintf(`not true-CE for query=%v, est=%v, act=%v`, q, estRows, actRows))
		}
		if rootExecInfo == "" {
			rootExecInfo, planCost = execInfo, cost
		}
		if planLabel == "Unmatched" {
			for _, operator := range []string{"Point", "Batch", "IndexReader", "IndexLookup", "TableReader", "Sort"} {
				if strings.Contains(strings.ToLower(id), strings.ToLower(operator)) {
					planLabel = operator
				}
			}
		}
	}
	if err := rs.Close(); err != nil {
		panic(err)
	}

	timeMS = parseTimeFromExecInfo(rootExecInfo)
	return
}

func parseTimeFromExecInfo(execInfo string) (timeMS float64) {
	// time:252.5ms, loops:99, index_task: {total_time: 13
	timeField := strings.Split(execInfo, ",")[0]
	timeField = strings.Split(timeField, ":")[1]
	dur, err := time.ParseDuration(timeField)
	if err != nil {
		panic(fmt.Sprintf("invalid time %v", timeField))
	}
	return float64(dur) / float64(time.Millisecond)
}
