package cost

import (
	"fmt"

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

	qs := genSyntheticCalibrationQueries(ins, "synthetic")
	for _, q := range qs {
		fmt.Println(q.SQL, q.Weights)
	}

	// TODO: run qs and get rs

	// TODO: do regression over rs
}
