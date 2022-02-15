package cost

import (
	"fmt"
	"strings"
	"time"
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

func NewCostWeights(cpu, copCPU, net, scan, descScan, mem, seek float64) CostWeights {
	return CostWeights{cpu, copCPU, net, scan, descScan, mem, seek}
}

type CaliRecord struct {
	SQL     string
	Label   string
	Weights CostWeights
	TimeNS  float64
	Cost    float64
}

type CaliRecords []CaliRecord

// CostCalibration ...
func CostCalibration() {
	recordsPath := "./cost-calibration-data/cost-calibration-data/synthetic-original-records.json"
	var rs Records
	if err := readFrom(recordsPath, &rs); err != nil {
		panic(err)
	}
	var crs CaliRecords
	for _, r := range rs {
		crs = append(crs, CaliRecord{
			SQL:     r.SQL,
			Label:   r.Label,
			Weights: r.CostWeights,
			TimeNS:  r.TimeMS * float64(time.Millisecond/time.Nanosecond),
			Cost:    r.Cost,
		})
	}

	whiteList := []string{
		"TableScan",
		"IndexScan",
		"WideTableScan",
		"WideIndexScan",
		"DescTableScan",
		"DescIndexScan",
		//"IndexLookup",
		//"Wide-IndexLookup",
		//"Agg-PushedDown",
		//"Agg-NotPushedDown",
		//"Sort",
	}
	crs = filterCaliRecordsByLabel(crs, whiteList, nil)
	// (CPU, CopCPU, Net, Scan, DescScan, Mem, Seek)
	//crs = maskRecords(crs, [NumFactors]bool{true, false, true, true, false, false, false})
	ret := regressionCostFactors(crs)
	fmt.Println(ret.String())
}

func maskRecords(rs CaliRecords, mask [NumFactors]bool) CaliRecords {
	ret := make(CaliRecords, 0, len(rs))
	for _, r := range rs {
		for k := 0; k < NumFactors; k++ {
			if mask[k] == false {
				r.Weights[k] = 0
			}
		}
		ret = append(ret, r)
	}
	return ret
}

func filterCaliRecordsByLabel(rs CaliRecords, whiteList, blackList []string) CaliRecords {
	ret := make(CaliRecords, 0, len(rs))
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
