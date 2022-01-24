package cost

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

const NumFactors = 7

type CostFactors [NumFactors]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)

func (fv CostFactors) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f, Seek: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5], fv[6])
}

type CostWeights [NumFactors]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)

func (fv CostWeights) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f, Seek: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5], fv[6])
}

func NewCostWeights(cpu, copCPU, net, scan, descScan, mem, seek float64) CostWeights {
	return CostWeights{cpu, copCPU, net, scan, descScan, mem, seek}
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

	var rs CaliRecords
	costFactors := readCostFactors(ins)
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read cali-records file error: ", err)

		ins.MustExec(fmt.Sprintf(`use %v`, db))
		ins.MustExec(`set @@tidb_index_lookup_size=1024`)
		ins.MustExec(`set @@tidb_cost_calibration_mode=2`)
		ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
		ins.MustExec(`set @@tidb_executor_concurrency=1`)
		ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
		ins.MustExec(`set @@tidb_cost_variant=1`)
		rs = make(CaliRecords, 0, len(qs))
		for i := range qs {
			// check cost weights
			calCost := calculateCost(qs[i].Weights, costFactors)

			_, planCost, timeMS, _ := extractCostTimeFromQuery(ins, qs[i].SQL, 10, 0, false)
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

	/*
		Table/IndexScan + Wide-Table/IndexScan ==> Scan = 25 * Net
		IndexScan + DescIndexScan ==> DescScan = 1.5 * Scan
		TableScan + DescTableScan ==> DescScan = 1.8 * Scan
		Table/IndexScan/Lookup + Wide-* ==> Seek = 1750000 * Net
		IndexScan + Wide-IndexScan + Agg-NotPushedDown + Sort ==> CPU = 7 * Net

		(CPU,	CopCPU,	Net,	Scan,	DescScan,	Mem,	Seek)
		(30,	30,		4,		100,	150,		0,		1750000)
	*/
	whilteList := []string{
		"TableScan",
		"IndexScan",
		//"IndexLookup",
		"Wide-TableScan",
		"Wide-IndexScan",
		//"Wide-IndexLookup",
		//"DescTableScan",
		//"DescIndexScan",
		//"Agg-PushedDown",
		"Agg-NotPushedDown",
		"Sort",
	}

	rs = filterCaliRecordsByLabel(rs, whilteList, nil)
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	rs = maskRecords(rs, [NumFactors]bool{true, false, true, true, false, false, false})

	ret := regressionCostFactors(rs)
	fmt.Println(ret.String())
}

func maskRecords(rs CaliRecords, mask [NumFactors]bool) CaliRecords {
	ret := make(CaliRecords, 0, len(rs))
	for _, r := range rs {
		for k := 0; k < NumFactors; k++ {
			if mask[k] == false {
				r.Weights[k] = 0
			}
		}
		ret = append(ret, r)
	}
	return ret
}

func filterCaliRecordsByLabel(rs CaliRecords, whiteList, blackList []string) CaliRecords {
	ret := make(CaliRecords, 0, len(rs))
	for _, r := range rs {
		if whiteList != nil {
			for _, label := range whiteList {
				if strings.ToLower(r.Label) == strings.ToLower(label) {
					ret = append(ret, r)
					break
				}
			}
		} else if blackList != nil {
			ok := true
			for _, label := range blackList {
				if strings.ToLower(r.Label) == strings.ToLower(label) {
					ok = false
					break
				}
			}
			if ok {
				ret = append(ret, r)
			}
		}
	}
	return ret
}
