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

func TestDrawBiasBoxPlotGroupByQueryType(t *testing.T) {
	opt := cetest.Option{
		QueryTypes: []string{"multi-cols-point-query"},
		Datasets: []cetest.DatasetOpt{
			{"imdb", "imdb"},
			{"zipfx", "zipfx"},
			{"tpcc", "tpcc"},
		},
		Instances: []tidb.Option{
			{Label: "v3.0"},
			{Label: "v4.0"},
			{Label: "no-CMSketch"},
		},
		ReportDir: "./test",
	}

	collector := cetest.NewEstResultCollector(3, 3, 1)
	for insIdx := 0; insIdx < 3; insIdx++ {
		for dsIdx := 0; dsIdx < 3; dsIdx++ {
			rs := randEstResult(100)
			for _, r := range rs {
				collector.AddEstResult(insIdx, dsIdx, 0, r)
			}
		}
	}
	fmt.Println(cetest.DrawBiasBoxPlotGroupByQueryType(opt, collector, 0))
}
