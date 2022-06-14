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
	//opt.Port = 4001

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	opts := []*evalOpt{
		//{"imdb", "imdb", "original", 2, 1, 3000},
		//{"imdb", "imdb", "calibrated", 30, 2, 3000},
		//{"tpch1g", "tpch", "original", 2, 1, 2000},
		//{"tpch1g", "tpch", "calibrated", 30, 2, 2000},
		//{"synthetic", "synthetic", "original", 20, 2, 300},
		{"synthetic", "synthetic", 1, 10, 3, 2000},
		//{"synthetic", "synthetic", "calibrating", 30, 3, 200},
	}

	for _, opt := range opts {
		evalOnDataset(ins, opt)
	}
	//drawSummary(opts)
	//genSyntheticData(ins, 1000000, "synthetic")
}

type evalOpt struct {
	db                 string
	dataset            string
	costModelVer       int
	queryScale         int
	processRepeat      int
	processTimeLimitMS int
}

func (opt *evalOpt) InitSQLs() []string {
	initSQLs := []string{
		`set @@tidb_distsql_scan_concurrency=1`,
		`set @@tidb_executor_concurrency=1`,
		`set @@tidb_opt_tiflash_concurrency_factor=1`,
	}
	switch opt.costModelVer {
	case 2:
		initSQLs = append(initSQLs, `set @@tidb_enable_new_cost_interface=1`, `set @@tidb_cost_model_version=2`)
	case 1:
		initSQLs = append(initSQLs, `set @@tidb_enable_new_cost_interface=1`, `set @@tidb_cost_model_version=1`)
	}
	return initSQLs
}

func (opt *evalOpt) GenQueries(ins tidb.Instance) Queries {
	switch strings.ToLower(opt.dataset) {
	case "imdb":
		return genIMDBEvaluationQueries(ins, opt.db, opt.queryScale)
	case "synthetic":
		return genSyntheticEvalQueries(ins, opt.db, opt.queryScale)
	case "tpch":
		return genTPCHEvaluationQueries(ins, opt.db, opt.queryScale)
	default:
		panic(opt.dataset)
	}
}

func evalOnDataset(ins tidb.Instance, opt *evalOpt) {
	fmt.Println("[cost-eval] start cost model evaluation ", opt.db, opt.dataset, opt.costModelVer)
	var qs Queries
	dataDir := "./cost-calibration-data"
	queryFile := filepath.Join(dataDir, fmt.Sprintf("%v-queries.json", opt.db))
	if err := readFrom(queryFile, &qs); err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = opt.GenQueries(ins)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), opt.db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	for _, sql := range opt.InitSQLs() {
		fmt.Println(sql + ";")
	}

	var rs Records
	recordFile := filepath.Join(dataDir, fmt.Sprintf("%v-%v-records.json", opt.db, opt.costModelVer))
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)
		rs = runCostEvalQueries(ins, opt.db, qs, opt.InitSQLs(), opt.processRepeat, opt.processTimeLimitMS)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	sort.Slice(rs, func(i, j int) bool {
		return rs[i].TimeMS < rs[j].TimeMS
	})

	tmp := make(Records, 0, len(rs))
	for _, r := range rs {
		if r.Label == "MPP2PhaseAgg" {
			continue
		}
		//if r.TimeMS > 1250 {
		//	continue
		//}
		fmt.Printf("[Record] %vms \t %.2f \t %v \t %v\n", r.TimeMS, r.Cost, r.Label, r.SQL)
		tmp = append(tmp, r)
	}

	drawCostRecordsTo(tmp, fmt.Sprintf("%v-%v-scatter.png", opt.db, opt.costModelVer))
	corr := KendallCorrelationByRecords(tmp)
	fmt.Printf("[cost-eval] KendallCorrelation %v-%v=%v \n", opt.db, opt.costModelVer, corr)
}

func drawSummary(opts []*evalOpt) {
	for _, ver := range []int{1, 2} {
		rs := make(Records, 0, 1024)
		for _, opt := range opts {
			if opt.costModelVer != ver {
				continue
			}

			recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-%v-records.json", opt.db, opt.costModelVer))
			var records Records
			if err := readFrom(recordFile, &records); err != nil {
				panic(fmt.Sprintf("read records from %v error: %v", recordFile, err))
			}

			var tmp Records
			for _, r := range records {
				if r.Label == "IndexLookup" || r.Label == "HashJoin" || r.Label == "MergeJoin" {
					continue
				}
				//if opt.dataset == "synthetic" && r.TimeMS > 200 {
				//	continue
				//}
				//fmt.Printf("[Record] %vms \t %.2f \t %v \t %v\n", r.TimeMS, r.Cost, r.Label, r.SQL)
				tmp = append(tmp, r)
			}

			rs = append(rs, tmp...)
		}
		drawCostRecordsTo(rs, fmt.Sprintf("%v-%v-scatter.png", "summary", ver))
		corr := KendallCorrelationByRecords(rs)
		fmt.Printf("[cost-eval] KendallCorrelation summary-%v=%v \n", ver, corr)
	}
}

type Query struct {
	PreSQLs []string
	SQL     string
	Label   string
	TypeID  int
}

type PlanChecker func(rawPlan []string) (reason string, ok bool)

type Queries []Query

type Record struct {
	Cost        float64
	TimeMS      float64
	Label       string
	SQL         string
	CostWeights CostWeights
}

type Records []Record

func runCostEvalQueries(ins tidb.Instance, db string, qs Queries, initSQLs []string, processRepeat, processTimeLimitMS int) Records {
	beginAt := time.Now()
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	for _, q := range initSQLs {
		fmt.Printf("[cost-eval] init SQL %v;\n", q)
		ins.MustExec(q)
	}

	records := make([]Record, 0, len(qs))
	i := 0
	for i < len(qs) {
		q := qs[i]
		fmt.Printf("[cost-eval] run query %v %v/%v %v\n", q, i, len(qs), time.Since(beginAt))

		for _, sql := range q.PreSQLs {
			ins.MustExec(sql)
		}

		query := `explain analyze format='true_card_cost' ` + q.SQL
		var label string
		var planCost, timeMS float64
		var cw CostWeights
		label, planCost, timeMS, tle, cw := extractCostTimeFromQuery(ins, query, processRepeat, processTimeLimitMS, true, getPlanChecker(q.Label))
		if tle { // skip all queries with the same TypeID
			fmt.Println("[cost-eval] skip TLE queries")
			tid := q.TypeID
			for i < len(qs) && qs[i].TypeID == tid {
				i++
			}
			continue
		}

		if q.Label != "" {
			label = q.Label
		}
		records = append(records, Record{
			Cost:        planCost,
			TimeMS:      timeMS,
			Label:       label,
			SQL:         query,
			CostWeights: cw,
		})
		i++
	}

	return records
}
