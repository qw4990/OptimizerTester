package cost

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/qw4990/OptimizerTester/tidb"
)

type FactorVector [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

type FactorWeightsVector [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)

type CaliQuery struct {
	SQL     string
	Label   string
	Weights FactorWeightsVector
}

type CaliQueries []CaliQuery

type CaliRecord struct {
	CaliQuery
	TimeMS float64
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
				TimeMS:    float64(timeCost) / float64(time.Millisecond),
			})

			fmt.Printf("[cost-eval] run %v/%v %v %v\n", i, len(qs), timeCost, qs[i].SQL)
		}
		rs = nil
		fmt.Printf("[cost-eval] gen %v cali-records for %v\n", len(qs), db)
		saveTo(recordFile, rs)
	} else {
		fmt.Println("[cost-eval] read cali-records from file successfully")
	}

	//regressionCostFactors(CaliRecords{
	//	{FactorWeightsVector{0, 0, 1, 1, 0, 0}, 1},
	//	{FactorWeightsVector{0, 0, 1, 2, 0, 0}, 2},
	//})
}
