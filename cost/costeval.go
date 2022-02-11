package cost

import (
	"fmt"
	"os"
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
		//{"imdb", "imdb", "original", 2, 1, 3000},
		//{"imdb", "imdb", "calibrated", 30, 2, 3000},
		//{"tpch1g", "tpch", "original", 2, 1, 2000},
		//{"tpch1g", "tpch", "calibrated", 30, 2, 2000},
		{"synthetic", "synthetic", "original", 2, 1, 500},
		//{"synthetic", "synthetic", "calibrated", 30, 2, 500},
	}

	for _, opt := range opts {
		evalOnDataset(ins, opt)
	}
	//drawSummary(opts)

	//genSyntheticData(ins, 100000, "synthetic")
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
		}
	} else {
		initSQLs = []string{
			`set @@tidb_index_lookup_size=1024`,
			`set @@tidb_distsql_scan_concurrency=1`,
			`set @@tidb_executor_concurrency=1`,
			`set @@tidb_opt_tiflash_concurrency_factor=1`,
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
		rs = runCostEvalQueries(ins, opt.db, qs, opt.InitSQLs(), opt.Factors(), opt.processRepeat, opt.processTimeLimitMS)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	os.Exit(0)

	sort.Slice(rs, func(i, j int) bool {
		return rs[i].TimeMS < rs[j].TimeMS
	})

	tmp := make(Records, 0, len(rs))
	for _, r := range rs {
		if r.Label == "IndexLookup" {
			continue
		}
		if opt.dataset == "synthetic" && r.TimeMS > 200 {
			continue
		}
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
				if opt.dataset == "synthetic" && r.TimeMS > 200 {
					continue
				}
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
	SQL    string
	Label  string
	TypeID int
}

type Queries []Query

type Record struct {
	Cost   float64
	TimeMS float64
	Label  string
	SQL    string
}

type Records []Record

func runCostEvalQueries(ins tidb.Instance, db string, qs Queries, initSQLs []string, factors *CostFactors, processRepeat, processTimeLimitMS int) Records {
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
	i := 0
	for i < len(qs) {
		q := qs[i]
		fmt.Printf("[cost-eval] run query %v %v/%v %v\n", q, i, len(qs), time.Since(beginAt))

		trueCardQuery := injectTrueCardinality(ins, q.SQL)

		label, planCost, timeMS, tle := extractCostTimeFromQuery(ins, trueCardQuery, processRepeat, processTimeLimitMS, true)
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
			Cost:   planCost,
			TimeMS: timeMS,
			Label:  label,
			SQL:    q.SQL,
		})
		i++
	}

	return records
}

func injectTrueCardinality(ins tidb.Instance, query string) string {
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
		if explainResult.UseTrueCardinality() {
			return injectedQuery
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
