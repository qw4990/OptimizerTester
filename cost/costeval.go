package cost

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
	"path/filepath"
	"sort"
	"sync"
	"time"
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
	opt.Addr = "127.0.0.1"

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	//genSyntheticData(ins, 100000, "synthetic")
	evalOnDataset(ins, "synthetic", genSyntheticQueries)
	//evalOnDataset(ins, "imdb", genIMDBQueries)
}

func evalOnDataset(ins tidb.Instance, db string, queryGenFunc func(ins tidb.Instance, db string) Queries) {
	fmt.Println("[cost-eval] start to eval on ", db)
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-queries.json", db))
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-records.json", db))

	qs, err := readQueriesFrom(queryFile)
	if err != nil {
		fmt.Println("[cost-eval] read queries file error: ", err)
		qs = queryGenFunc(ins, db)
		fmt.Printf("[cost-eval] gen %v queries for %v\n", len(qs), db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read queries from file successfully ")
	}

	all, err := readRecordsFrom(recordFile)
	if err != nil {
		fmt.Println("[cost-eval] read records file error: ", err)

		concurrency := 1
		instances := make([]tidb.Instance, concurrency)
		for i := 0; i < concurrency; i++ {
			tmp, err := tidb.ConnectTo(ins.Opt())
			if err != nil {
				panic(err)
			}
			instances[i] = tmp
		}

		var wg sync.WaitGroup
		queries := splitQueries(qs, concurrency)
		rs := make([]Records, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				defer fmt.Printf("[cost-eval] worker-%v finish\n", id)
				rs[id] = runCostEvalQueries(id, instances[id], db, queries[id])
			}(i)
		}
		wg.Wait()

		for _, r := range rs {
			all = append(all, r...)
		}
		saveRecordsTo(all, recordFile)
	} else {
		fmt.Println("[cost-eval] read records from file successfully")
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].TimeMS < all[j].TimeMS
	})

	tmp := make(Records, 0, len(all))
	for _, r := range all {
		if r.Label == "Point" {
			continue
		}
		//if r.Cost < 4e8 || r.Cost > 7e8 || r.TimeMS > 3500 {
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

func runCostEvalQueries(id int, ins tidb.Instance, db string, qs Queries) Records {
	beginAt := time.Now()
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	ins.MustExec(`set @@tidb_cost_calibration_mode=2`)
	ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
	ins.MustExec(`set @@tidb_executor_concurrency=1`)
	ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)

	ins.MustExec(`set @@tidb_opt_cpu_factor=123`)
	ins.MustExec(`set @@tidb_opt_copcpu_factor=0`)
	ins.MustExec(`set @@tidb_opt_network_factor=3.37`)
	ins.MustExec(`set @@tidb_opt_scan_factor=3.98`)
	ins.MustExec(`set @@tidb_opt_desc_factor=0`)
	ins.MustExec(`set @@tidb_opt_memory_factor=0`)
	records := make([]Record, 0, len(qs))

	for i, q := range qs {
		fmt.Printf("[cost-eval] worker-%v run query %v %v/%v %v\n", id, q, i, len(qs), time.Since(beginAt))
		planLabel := "Unmatched"
		planCost, timeMS := extractCostTimeFromQuery(ins, q.SQL, 3, true)

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

func splitQueries(r Queries, n int) []Queries {
	rs := make([]Queries, n)
	for i, record := range r {
		rs[i%n] = append(rs[i%n], record)
	}
	return rs
}
