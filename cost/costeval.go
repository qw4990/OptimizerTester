package cost

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

// CostEval ...
func CostEval() {
	opt := tidb.Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}
	//opt.Addr = "127.0.0.1"

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	var factors *CostFactors
	var initSQLs []string
	testCalibrated := false

	if testCalibrated {
		//(CPU,	CopCPU,	Net,	Scan,	DescScan,	Mem,	Seek)
		//(30,	30,		4,		100,	150,		0,		1.2*1e7)
		factors = &CostFactors{30, 30, 4, 100, 150, 0, 1.2 * 1e7}
		initSQLs = []string{
			`set @@tidb_index_lookup_size=1024`,
			`set @@tidb_distsql_scan_concurrency=1`,
			`set @@tidb_executor_concurrency=1`,
			`set @@tidb_opt_tiflash_concurrency_factor=1`,
			`set @@tidb_cost_calibration_mode=2`, // use true-CE
			`set @@tidb_cost_variant=1`,          // use the new cost model
		}
	} else {
		factors = nil
		initSQLs = []string{
			`set @@tidb_index_lookup_size=1024`,
			`set @@tidb_distsql_scan_concurrency=1`,
			`set @@tidb_executor_concurrency=1`,
			`set @@tidb_opt_tiflash_concurrency_factor=1`,
			`set @@tidb_cost_calibration_mode=2`, // use true-CE
			`set @@tidb_cost_variant=0`,          // use the original cost model
		}
	}

	//genSyntheticData(ins, 100000, "synthetic")
	evalOnDataset(ins, "synthetic", factors, initSQLs, genSyntheticQueries)
	//evalOnDataset(ins, "imdb", factors, initSQLs, genIMDBQueries)
}

func evalOnDataset(ins tidb.Instance, db string, factors *CostFactors, initSQLs []string,
	queryGenFunc func(ins tidb.Instance, db string) Queries) {
	fmt.Println("[cost-eval] start to eval on ", db)
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-queries.json", db))
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-records.json", db))

	var qs Queries
	if err := readFrom(queryFile, &qs); err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = queryGenFunc(ins, db)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	var rs Records
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)
		rs = runCostEvalQueries(0, ins, db, qs, initSQLs, factors)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	sort.Slice(rs, func(i, j int) bool {
		return rs[i].TimeMS < rs[j].TimeMS
	})

	tmp := make(Records, 0, len(rs))
	for _, r := range rs {
		if r.Label == "Point" {
			continue
		}
		//if r.Cost > 2.5e8 {
		//	continue
		//}
		fmt.Println(">>>> ", r.SQL, r.Cost, r.TimeMS)
		//if r.Cost < 1000 { // the cost of PointGet is always zero
		//	continue
		//}
		tmp = append(tmp, r)
	}

	drawCostRecordsTo(tmp, fmt.Sprintf("%v-scatter.png", db))
}

type Query struct {
	SQL   string
	Label string
}

type Queries []Query

type Record struct {
	Cost   float64
	TimeMS float64
	Label  string
	SQL    string
}

type Records []Record

func runCostEvalQueries(id int, ins tidb.Instance, db string, qs Queries, initSQLs []string, factors *CostFactors) Records {
	beginAt := time.Now()
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	for _, q := range initSQLs {
		ins.MustExec(q)
	}

	if factors != nil {
		setCostFactors(ins, *factors)
		check := readCostFactors(ins)
		if check != *factors {
			panic("set factor failed")
		}
	}

	records := make([]Record, 0, len(qs))
	for i, q := range qs {
		fmt.Printf("[cost-eval] worker-%v run query %v %v/%v %v\n", id, q, i, len(qs), time.Since(beginAt))
		planLabel := "Unmatched"
		planCost, timeMS := extractCostTimeFromQuery(ins, q.SQL, 5, true)

		if q.Label != "" {
			planLabel = q.Label
		}
		records = append(records, Record{
			Cost:   planCost,
			TimeMS: timeMS,
			Label:  planLabel,
			SQL:    q.SQL,
		})
	}

	return records
}
