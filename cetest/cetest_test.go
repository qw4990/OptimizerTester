package cetest_test

import (
	"fmt"
	"testing"

	"github.com/qw4990/OptimizerTester/cetest"
	"github.com/qw4990/OptimizerTester/tidb"
)

func TestGenReport(t *testing.T) {
	opt := cetest.Option{
		QueryTypes: []string{"multi-cols-point-query", "single-col-point-query"},
		Datasets: []cetest.DatasetOpt{
			{Label: "zipfx"},
			{Label: "tpcc-10G"},
			{Label: "tpcc-100G"},
			{Label: "imdb"},
		},
		Instances: []tidb.Option{
			{Label: "v3.0"},
			{Label: "v4.0"},
			{Label: "no-CMSketch"},
		},
		ReportDir: "./test",
	}

	collector := randEstResultCollector(opt, 100)
	fmt.Println(cetest.GenReport(opt, collector))
}
