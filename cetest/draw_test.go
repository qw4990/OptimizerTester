package cetest_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/qw4990/OptimizerTester/cetest"
	"github.com/qw4990/OptimizerTester/tidb"
)

func randEstResult(n int) []cetest.EstResult {
	rs := make([]cetest.EstResult, 0, n)
	for i := 0; i < n; i++ {
		trueCard := rand.NormFloat64() * 1000000
		bias := rand.NormFloat64()
		estCard := trueCard*bias + trueCard
		rs = append(rs, cetest.EstResult{
			EstCard:  estCard,
			TrueCard: trueCard,
		})
	}
	return rs
}

func randEstResultCollector(opt cetest.Option, n int) cetest.EstResultCollector {
	collector := cetest.NewEstResultCollector(len(opt.Instances), len(opt.Datasets), len(opt.QueryTypes))
	for insIdx := 0; insIdx < len(opt.Instances); insIdx++ {
		for dsIdx := 0; dsIdx < len(opt.Datasets); dsIdx++ {
			for qtIdx := 0; qtIdx < len(opt.QueryTypes); qtIdx++ {
				rs := randEstResult(n)
				for _, r := range rs {
					collector.AddEstResult(insIdx, dsIdx, qtIdx, r)
				}
			}
		}
	}
	return collector
}

func TestDrawBiasBoxPlotGroupByQueryType(t *testing.T) {
	opt := cetest.Option{
		QueryTypes: []string{"multi-cols-point-query"},
		Datasets: []cetest.DatasetOpt{
			{Label:"zipfx"},
			{Label:"tpcc-10G"},
			{Label:"tpcc-100G"},
			{Label:"imdb"},
		},
		Instances: []tidb.Option{
			{Label: "v3.0"},
			{Label: "v4.0"},
			{Label: "no-CMSketch"},
		},
		ReportDir: "./test",
	}

	collector := randEstResultCollector(opt, 100)
	fmt.Println(cetest.DrawBiasBoxPlotGroupByQueryType(opt, collector, 0))
}
