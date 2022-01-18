package cost

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

type CostFactors [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

func (fv CostFactors) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5])
}

type CostWeights [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

func (fv CostWeights) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5])
}

type CaliQuery struct {
	SQL     string
	Label   string
	Weights CostWeights
}

type CaliQueries []CaliQuery

type CaliRecord struct {
	CaliQuery
	TimeNS float64
	Cost   float64
}

type CaliRecords []CaliRecord

// CostCalibration ...
func CostCalibration() {
	opt := tidb.Option{
		Addr:     "172.16.5.173",
		Port:     4000,
		User:     "root",
		Password: "",
		Label:    "",
	}

	ins, err := tidb.ConnectTo(opt)
	if err != nil {
		panic(err)
	}

	db := "synthetic"
	queryFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-caliqueries.json", db))
	recordFile := filepath.Join("/tmp/cost-calibration", fmt.Sprintf("%v-calirecords.json", db))

	var qs CaliQueries
	if err := readFrom(queryFile, &qs); err != nil {
		fmt.Println("[cost-eval] read cali-queries file error: ", err)
		qs = genSyntheticCalibrationQueries(ins, db)
		fmt.Printf("[cost-eval] gen %v cali-queries for %v\n", len(qs), db)
		saveTo(queryFile, qs)
	} else {
		fmt.Println("[cost-eval] read cali-queries from file successfully ")
	}

	//for _, q := range qs {
	//	fmt.Println(q.SQL, q.Weights)
	//}
	//os.Exit(0)

	var rs CaliRecords
	costFactors := readCostFactors(ins)
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read cali-records file error: ", err)

		ins.MustExec(fmt.Sprintf(`use %v`, db))
		ins.MustExec(`set @@tidb_cost_calibration_mode=2`)
		ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
		ins.MustExec(`set @@tidb_executor_concurrency=1`)
		ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
		ins.MustExec(`set @@tidb_cost_variant=1`)
		rs = make(CaliRecords, 0, len(qs))
		for i := range qs {
			// check cost weights
			calCost := calculateCost(qs[i].Weights, costFactors)

			planCost, timeMS := extractCostTimeFromQuery(ins, qs[i].SQL, 10, false)
			rs = append(rs, CaliRecord{
				CaliQuery: qs[i],
				Cost:      planCost,
				TimeNS:    timeMS * float64(time.Millisecond/time.Nanosecond),
			})
			fmt.Printf("[cost-eval] run %v/%v timeMS:%v, SQL:%v, costAct:%v, costCal: %v\n", i, len(qs), timeMS, qs[i].SQL, planCost, calCost)

			costDelta := math.Abs(planCost - calCost)
			if costDelta > 50 && costDelta/planCost > 0.05 {
				panic("wrong cal-cost")
			}
		}
		fmt.Printf("[cost-eval] gen %v cali-records for %v\n", len(qs), db)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read cali-records from file successfully")
	}

	for _, r := range rs {
		fmt.Println(r.SQL, r.Weights, r.Cost, time.Duration(r.TimeNS)*time.Nanosecond)
	}
	//os.Exit(0)

	//rs = filterCaliRecordsByLabel(rs, "IndexScan", "wide-indexscan")
	//rs = filterCaliRecordsByLabel(rs, "IndexScan", "TableScan", "IndexLookup", "wide-tablescan", "wide-indexscan")
	//rs = rs[:2]

	ret := regressionCostFactors(rs)
	fmt.Println(ret.String())
}

func filterCaliRecordsByLabel(rs CaliRecords, labels ...string) CaliRecords {
	ret := make(CaliRecords, 0, len(rs))
	for _, r := range rs {
		for _, label := range labels {
			if strings.ToLower(r.Label) == strings.ToLower(label) {
				ret = append(ret, r)
				break
			}
		}
	}
	return ret
}
