package cost_test

import (
	"fmt"
	"testing"

	"github.com/qw4990/OptimizerTester/cost"
)

func TestPearsonCorrelation(t *testing.T) {
	xs := []float64{0, 1, 2, 3, 4, 5}
	ys := []float64{6, 7, 8, 9, 10, 11}
	if cost.PearsonCorrelation(xs, ys) != 1 {
		panic("???")
	}
	if cost.KendallCorrelation(xs, ys) != 1 {
		panic("???")
	}

	ys = []float64{90000, 800, 700, 60, 50, 4}
	fmt.Println(cost.PearsonCorrelation(xs, ys))
	fmt.Println(cost.KendallCorrelation(xs, ys))
}
