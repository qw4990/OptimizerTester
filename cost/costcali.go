package cost

import "github.com/qw4990/OptimizerTester/tidb"

type CaliQuery struct {
	SQL          string
	Label        string
	FactorVector [6]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem)
}

type CaliQueries []CaliQuery

type CaliRecord struct {
	CaliQuery
	TimeMS float64
}

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
}
