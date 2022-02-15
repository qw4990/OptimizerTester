package cost

import (
	"fmt"
	"strings"
)

const NumFactors = 7

type CostFactors [NumFactors]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)

func (fv CostFactors) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f, Seek: %.2f]", fv[0], fv[1], fv[2], fv[3], fv[4], fv[5], fv[6])
}

type CostWeights [NumFactors]float64 // (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)

func (cw CostWeights) String() string {
	return fmt.Sprintf("[CPU: %.2f, copCPU: %.2f, Net: %.2f, Scan: %.2f, DescScan: %.2f, Mem: %.2f, Seek: %.2f]", cw[0], cw[1], cw[2], cw[3], cw[4], cw[5], cw[6])
}

func (cw CostWeights) EqualTo(that CostWeights) bool {
	for i := range cw {
		if cw[i] != that[i] {
			return false
		}
	}
	return true
}

func (cw CostWeights) IsZero() bool {
	for _, v := range cw {
		if v != 0 {
			return false
		}
	}
	return true
}

func (cw CostWeights) CalCost(factors CostFactors) float64 {
	var cost float64
	for i := range cw {
		cost += cw[i] * factors[i]
	}
	return cost
}

// CostCalibration ...
func CostCalibration() {
	recordsPath := "./cost-calibration-data/synthetic-calibrating-records.json"
	var rs Records
	if err := readFrom(recordsPath, &rs); err != nil {
		panic(err)
	}

	whiteList := []string{
		// TiKV Plans
		"TableScan",
		"IndexScan",
		"WideTableScan",
		"WideIndexScan",
		"DescTableScan",
		"DescIndexScan",
		"StreamAgg",
		"HashAgg",
		"Sort",
		"HashJoin",
		"MergeJoin",
		//"IndexLookup",
		//"WideIndexLookup",

		// TiFlash Plans
		"TiFlashScan",
		"MPPScan",
		"MPPTiDBAgg",
		"MPP2PhaseAgg",
		"MPPHJ",
		"MPPBCJ",
	}
	rs = filterCaliRecordsByLabel(rs, whiteList, nil)

	// ====== Manual Calibration ======
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	// (30,	30,		4,		100,	150,		0,		1.2*1e7)
	recalculateAndDraw(rs, &CostFactors{30, 30, 4, 100, 150, 0, 1.2 * 1e7})
	//recalculateAndDraw(rs, nil)

	// ====== Automatic Regression ======
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	//rs = maskRecords(rs, [NumFactors]bool{false, false, true, true, false, false, false})
	//ret := regressionCostFactors(rs)
	//fmt.Println(ret.String())
}

func recalculateAndDraw(rs Records, factors *CostFactors) {
	for i := range rs {
		if factors != nil {
			rs[i].Cost = rs[i].CostWeights.CalCost(*factors)
		}
	}

	drawCostRecordsTo(rs, fmt.Sprintf("%v-%v-scatter.png", "synthetic", "calibrating"))
}

func maskRecords(rs Records, mask [NumFactors]bool) Records {
	ret := make(Records, 0, len(rs))
	for _, r := range rs {
		for k := 0; k < NumFactors; k++ {
			if mask[k] == false {
				r.CostWeights[k] = 0
			}
		}
		ret = append(ret, r)
	}
	return ret
}

func filterCaliRecordsByLabel(rs Records, whiteList, blackList []string) Records {
	ret := make(Records, 0, len(rs))
	for _, r := range rs {
		if whiteList != nil {
			for _, label := range whiteList {
				if strings.ToLower(r.Label) == strings.ToLower(label) {
					ret = append(ret, r)
					break
				}
			}
		} else if blackList != nil {
			ok := true
			for _, label := range blackList {
				if strings.ToLower(r.Label) == strings.ToLower(label) {
					ok = false
					break
				}
			}
			if ok {
				ret = append(ret, r)
			}
		}
	}
	return ret
}
