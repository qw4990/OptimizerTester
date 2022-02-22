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
		{"synthetic", "synthetic", "calibrated", 20, 2, 300},
		//{"synthetic", "synthetic", "calibrating", 30, 3, 200},
	}

	for _, opt := range opts {
		//fmt.Println("--->>>> ", opt)
		evalOnDataset(ins, opt)
	}
	//drawSummary(opts)
	//genSyntheticData(ins, 1000000, "synthetic")
}

type evalOpt struct {
	db                 string
	dataset            string
	mode               string
	queryScale         int
	processRepeat      int
	processTimeLimitMS int
}

func (opt *evalOpt) Factors() *CostFactors {
	if opt.mode == "calibrated" {
		//(CPU,	CopCPU,	Net,	Scan,	DescScan,	Mem,	Seek, 		TiFlashScan)
		//(30,	30,		4,		100,	150,		0,		1.2*1e7, 	10)
		factors := &CostFactors{30, 30, 4, 100, 150, 0, 1.2 * 1e7, 15}
		return factors
	}
	return nil
}

func (opt *evalOpt) InitSQLs() []string {
	initSQLs := []string{
		`set @@tidb_distsql_scan_concurrency=1`,
		`set @@tidb_executor_concurrency=1`,
		`set @@tidb_opt_tiflash_concurrency_factor=1`,
	}
	switch strings.ToLower(opt.mode) {
	case "calibrated", "calibrating":
		initSQLs = append(initSQLs, `set @@tidb_cost_variant=1`)
	case "original":
		initSQLs = append(initSQLs, `set @@tidb_cost_variant=0`)
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
	fmt.Println("[cost-eval] start cost model evaluation ", opt.db, opt.dataset, opt.mode)
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

	//qs = filterQueriesByLabel(qs, []string{"TableScan", "IndexScan", "MPPScan"})

	var rs Records
	recordFile := filepath.Join(dataDir, fmt.Sprintf("%v-%v-records.json", opt.db, opt.mode))
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)
		rs = runCostEvalQueries(ins, opt.db, qs, opt.InitSQLs(), opt.Factors(), opt.processRepeat, opt.processTimeLimitMS)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	sort.Slice(rs, func(i, j int) bool {
		return rs[i].TimeMS < rs[j].TimeMS
	})

	tmp := make(Records, 0, len(rs))
	for _, r := range rs {
		if r.Label == "IndexLookup" {
			continue
		}
		//if strings.Contains(r.Label, "Wide") || strings.Contains(r.Label, "Desc") {
		//	continue
		//}
		//if opt.dataset == "synthetic" && r.TimeMS > 200 {
		//	continue
		//}
		fmt.Printf("[Record] %vms \t %.2f \t %v \t %v\n", r.TimeMS, r.Cost, r.Label, r.SQL)
		tmp = append(tmp, r)
	}

	drawCostRecordsTo(tmp, fmt.Sprintf("%v-%v-scatter.png", opt.db, opt.mode))
	corr := KendallCorrelationByRecords(tmp)
	fmt.Printf("[cost-eval] KendallCorrelation %v-%v=%v \n", opt.db, opt.mode, corr)
}

func drawSummary(opts []*evalOpt) {
	for _, mode := range []string{"calibrated", "original"} {
		rs := make(Records, 0, 1024)
		for _, opt := range opts {
			if strings.ToLower(opt.mode) != mode {
				continue
			}

			recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-%v-records.json", opt.db, opt.mode))
			var records Records
			if err := readFrom(recordFile, &records); err != nil {
				panic(fmt.Sprintf("read records from %v error: %v", recordFile, err))
			}

			var tmp Records
			for _, r := range records {
				if r.Label == "IndexLookup" {
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
		drawCostRecordsTo(rs, fmt.Sprintf("%v-%v-scatter.png", "summary", mode))
		corr := KendallCorrelationByRecords(rs)
		fmt.Printf("[cost-eval] KendallCorrelation summary-%v=%v \n", mode, corr)
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

func runCostEvalQueries(ins tidb.Instance, db string, qs Queries, initSQLs []string, factors *CostFactors, processRepeat, processTimeLimitMS int) Records {
	beginAt := time.Now()
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	for _, q := range initSQLs {
		fmt.Printf("[cost-eval] init SQL %v;\n", q)
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
	i := 0
	for i < len(qs) {
		q := qs[i]
		fmt.Printf("[cost-eval] run query %v %v/%v %v\n", q, i, len(qs), time.Since(beginAt))

		for _, sql := range q.PreSQLs {
			ins.MustExec(sql)
		}

		trueCardQuery, tle := injectTrueCardinality(ins, q.SQL, processTimeLimitMS)
		var label string
		var planCost, timeMS float64
		var cw CostWeights
		if !tle {
			label, planCost, timeMS, tle, cw = extractCostTimeFromQuery(ins, trueCardQuery, processRepeat, processTimeLimitMS, true, getPlanChecker(q.Label))
		}
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
			SQL:         trueCardQuery,
			CostWeights: cw,
		})
		i++
	}

	return records
}

func injectTrueCardinality(ins tidb.Instance, query string, timeLimitMS int) (string, bool) {
	query = "explain analyze " + injectHint(query, "display_cost(), trace_cost()")
	cardinality := make(map[string]float64)

	// we need to run and inject cardinality multiple times since the plan may change after changing the cardinality
	i := 0
	for {
		// inject current true cardinality into this query
		var cardHints []string
		for op, rows := range cardinality {
			cardHints = append(cardHints, fmt.Sprintf("true_cardinality(%v=%v)", op, int(rows)))
		}
		injectedQuery := query
		if len(cardHints) > 0 {
			sort.Strings(cardHints)
			injectedQuery = injectHint(query, strings.Join(cardHints, ", "))
		}

		// run this query and check whether estRows are equal to actRows
		rs := ins.MustQuery(injectedQuery)
		explainResult := ParseExplainAnalyzeResultsWithRows(rs)
		if timeLimitMS > 0 && int(explainResult.TimeMS) > timeLimitMS {
			return "", true
		}
		if i > 0 && explainResult.UseTrueCardinality() {
			return injectedQuery, false
		}

		fmt.Println("### ", injectedQuery)
		i++
		if i > 5 {
			panic("cannot get a stable plan")
		}

		// add current actRows into the true cardinality hints next time
		for op, act := range explainResult.OperatorActRows {
			cardinality[op] = act
		}
	}
}
