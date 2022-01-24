package cost

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
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

	processRepeat := 1
	db, dataset := "TPCH1G", "tpch"
	//mode := "baseline"
	mode := "calibrated"

	var factors *CostFactors
	if mode == "calibrated" {
		//(CPU,	CopCPU,	Net,	Scan,	DescScan,	Mem,	Seek)
		//(30,	30,		4,		100,	150,		0,		1.2*1e7)
		factors = &CostFactors{30, 30, 4, 100, 150, 0, 1.2 * 1e7}
	}

	//genSyntheticData(ins, 100000, "synthetic")
	evalOnDataset(ins, db, dataset, mode, factors, processRepeat)
}

func evalOnDataset(ins tidb.Instance, db, dataset, mode string, factors *CostFactors, processRepeat int) {
	var queryGener func(ins tidb.Instance, db string) Queries
	switch strings.ToLower(dataset) {
	case "imdb":
		queryGener = genIMDBEvaluationQueries
	case "synthetic":
		queryGener = genSyntheticEvaluationQueries
	case "tpch":
		queryGener = genTPCHEvaluationQueries
	default:
		panic(dataset)
	}

	var initSQLs []string
	if mode == "calibrated" {
		initSQLs = []string{
			`set @@tidb_index_lookup_size=1024`,
			`set @@tidb_distsql_scan_concurrency=1`,
			`set @@tidb_executor_concurrency=1`,
			`set @@tidb_opt_tiflash_concurrency_factor=1`,
			`set @@tidb_cost_calibration_mode=2`, // use true-CE
			`set @@tidb_cost_variant=1`,          // use the new cost model
		}
	} else {
		initSQLs = []string{
			`set @@tidb_index_lookup_size=1024`,
			`set @@tidb_distsql_scan_concurrency=1`,
			`set @@tidb_executor_concurrency=1`,
			`set @@tidb_opt_tiflash_concurrency_factor=1`,
			`set @@tidb_cost_calibration_mode=2`, // use true-CE
			`set @@tidb_cost_variant=0`,          // use the original cost model
		}
	}

	fmt.Println("[cost-eval] start to eval on ", db)
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-queries.json", db))
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-%v-records.json", db, mode))

	var qs Queries
	if err := readFrom(queryFile, &qs); err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = queryGener(ins, db)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	var rs Records
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)
		rs = runCostEvalQueries(ins, db, qs, initSQLs, factors, processRepeat)
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
		//if r.Cost < 5e8 {
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

func runCostEvalQueries(ins tidb.Instance, db string, qs Queries, initSQLs []string, factors *CostFactors, processRepeat int) Records {
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
		fmt.Printf("[cost-eval] run query %v %v/%v %v\n", q, i, len(qs), time.Since(beginAt))
		label, planCost, timeMS := extractCostTimeFromQuery(ins, q.SQL, processRepeat, true)
		if q.Label != "" {
			label = q.Label
		}
		records = append(records, Record{
			Cost:   planCost,
			TimeMS: timeMS,
			Label:  label,
			SQL:    q.SQL,
		})
	}

	return records
}
