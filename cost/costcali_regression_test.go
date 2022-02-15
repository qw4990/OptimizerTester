package cost

import (
	"fmt"
	"math"
	"testing"
)

func TestRegression(t *testing.T) {
	// case 1: time = (net * 1 + scan * 5) * 1000
	var rs CaliRecords
	for i := 0; i < 100; i++ {
		net := float64(i * 2)
		scan := math.Log(float64(i*100+100)) * float64(i*10)
		timeNS := (net + scan*5) * 1000
		rs = append(rs, CaliRecord{
			SQL:     "",
			Label:   "",
			Weights: CostWeights{0, 0, net, scan, 0, 0, 0}, // (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
			TimeNS:  timeNS,
			Cost:    0,
		})
	}

	ret := regressionCostFactors(rs)
	fmt.Println(ret)
}
