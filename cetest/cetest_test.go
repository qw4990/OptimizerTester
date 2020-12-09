package cetest_test

import (
	"testing"

	"github.com/qw4990/OptimizerTester/cetest"
	"github.com/qw4990/OptimizerTester/tidb"
)

func TestGenQErrorBoxPlotReport(t *testing.T) {
	opt := cetest.Option{
		QueryTypes: []cetest.QueryType{cetest.QTMultiColsPointQuery, cetest.QTMultiColsRangeQuery},
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
	if err := cetest.GenQErrorBoxPlotReport(opt, collector); err != nil {
		t.Fail()
	}
}

func TestGenPErrorBarChartsReport(t *testing.T) {
	opt := cetest.Option{
		QueryTypes: []cetest.QueryType{cetest.QTMultiColsPointQuery, cetest.QTMultiColsRangeQuery},
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
	if err := cetest.GenPErrorBarChartsReport(opt, collector); err != nil {
		t.Fail()
	}
}

func TestDecodeOption(t *testing.T) {
	content := `
query-types = ["multi-cols-point-query", "single-col-point-query"]
report-dir = "/tmp/xxx"

[[datasets]]
name = "imdb"
db = "imdb"
label = "imdb"

[[datasets]]
name = "tpcc"
db = "tpcc10G"
label = "tpcc10G"


[[datasets]]
name = "tpcc"
db = "tpcc50G"
label = "tpcc50G"

[[instances]]
addr = "127.0.0.1"
port = 4000
user = "root"
password = "123456"
label = "v4.0"


[[instances]]
addr = "127.0.0.1"
port = 4001
user = "root"
password = "123456"
label = "master"
`
	opt, err := cetest.DecodeOption(content)
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.QueryTypes) != 2 || len(opt.Datasets) != 3 || len(opt.Instances) != 2 ||
		opt.QueryTypes[0] != cetest.QTMultiColsPointQuery || opt.QueryTypes[1] != cetest.QTSingleColPointQuery {
		t.Fatal()
	}
}
