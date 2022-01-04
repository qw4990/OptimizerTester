package cost

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

type FactorVector [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

func (fv FactorVector) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5])
}

type FactorWeightsVector [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

func (fv FactorWeightsVector) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5])
}

type CaliQuery struct {
	SQL     string
	Label   string
	Weights FactorWeightsVector
}

type CaliQueries []CaliQuery

type CaliRecord struct {
	CaliQuery
	TimeNS float64
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
	opt.Addr = "127.0.0.1"

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
	if err := readFrom(recordFile, &rs); err != nil {
		fmt.Println("[cost-eval] read cali-records file error: ", err)

		ins.MustExec(fmt.Sprintf(`use %v`, db))
		ins.MustExec(`set @@tidb_cost_calibration_mode=0`)
		ins.MustExec(`set @@tidb_distsql_scan_concurrency=1`)
		ins.MustExec(`set @@tidb_executor_concurrency=1`)
		ins.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
		rs = make(CaliRecords, 0, len(qs))
		for i := range qs {
			begin := time.Now()
			query := "explain analyze " + qs[i].SQL
			rows := ins.MustQuery(query)
			timeCost := time.Since(begin) // client-time
			if err := rows.Close(); err != nil {
				panic(err)
			}
			rs = append(rs, CaliRecord{
				CaliQuery: qs[i],
				TimeNS:    float64(timeCost) / float64(time.Nanosecond),
			})

			fmt.Printf("[cost-eval] run %v/%v %v %v\n", i, len(qs), timeCost, qs[i].SQL)
		}
		fmt.Printf("[cost-eval] gen %v cali-records for %v\n", len(qs), db)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read cali-records from file successfully")
	}

	//rs = filterCaliRecordsByLabel(rs, "IndexScan")
	//rs = rs[:2]

	regressionCostFactors(rs)
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