package cetest_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/qw4990/OptimizerTester/cetest"
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

func TestDrawBiasBoxPlot(t *testing.T) {
	dsNames := []string{"imdb", "zipfx", "tpcc"}
	estResults := [][]cetest.EstResult{randEstResult(20), randEstResult(20), randEstResult(20)}
	fmt.Println(cetest.DrawBiasBoxPlot(dsNames, estResults, "multi-cols-point-query", "./test"))
}
