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

	opts := []*evalOpt{
		{"imdb", "imdb", "original", 20, 2},
		{"imdb", "imdb", "calibrated", 20, 2},
		//{"tpch1g", "tpch", "original", 20, 2},
		//{"tpch1g", "tpch", "calibrated", 20, 2},
		//{"synthetic", "synthetic", "original", 20, 2},
		//{"synthetic", "synthetic", "calibrated", 20, 2},
	}

	for _, opt := range opts {
		evalOnDataset(ins, opt)
	}
	//genSyntheticData(ins, 100000, "synthetic")
}

type evalOpt struct {
	db            string
	dataset       string
	mode          string
	queryScale    int
	processRepeat int
}

func (opt *evalOpt) Factors() *CostFactors {
	if opt.mode == "calibrated" {
		//(CPU,	CopCPU,	Net,	Scan,	DescScan,	Mem,	Seek)
		//(30,	30,		4,		100,	150,		0,		1.2*1e7)
		factors := &CostFactors{30, 30, 4, 100, 150, 0, 1.2 * 1e7}
		return factors
	}
	return nil
}

func (opt *evalOpt) InitSQLs() []string {
	var initSQLs []string
	if strings.ToLower(opt.mode) == "calibrated" {
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
	return initSQLs
}

func (opt *evalOpt) GenQueries(ins tidb.Instance) Queries {
	switch strings.ToLower(opt.dataset) {
	case "imdb":
		return genIMDBEvaluationQueries(ins, opt.db, opt.queryScale)
	case "synthetic":
		return genSyntheticEvaluationQueries(ins, opt.db, opt.queryScale)
	case "tpch":
		return genTPCHEvaluationQueries(ins, opt.db, opt.queryScale)
	default:
		panic(opt.dataset)
	}
}

func evalOnDataset(ins tidb.Instance, opt *evalOpt) {
	fmt.Println("[cost-eval] start cost model evaluation ", opt.db, opt.dataset, opt.mode)
	var qs Queries
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-queries.json", opt.db))
	if err := readFrom(queryFile, &qs); err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = opt.GenQueries(ins)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), opt.db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	var rs Records
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-%v-records.json", opt.db, opt.mode))
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)
		rs = runCostEvalQueries(ins, opt.db, qs, opt.InitSQLs(), opt.Factors(), opt.processRepeat)
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
		if r.Label == "IndexLookup" {
			continue
		}
		//if r.Cost < 5e8 {
		//	continue
		//}
		fmt.Printf("[Record] %vms \t %.2f \t %v \t %v\n", r.TimeMS, r.Cost, r.Label, r.SQL)
		//if r.Cost < 1000 { // the cost of PointGet is always zero
		//	continue
		//}
		tmp = append(tmp, r)
	}

	drawCostRecordsTo(tmp, fmt.Sprintf("%v-%v-scatter.png", opt.db, opt.mode))
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
