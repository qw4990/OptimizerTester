package cetest

import (
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

type EstResult struct {
	EstCard  float64 // estimated cardinality
	TrueCard float64 // true cardinality
}

// QError is max(est/true, true/est) or ((numerator+1)/(denominator+1)) if the denominator is 0.
func QError(r EstResult) float64 {
	if r.EstCard > r.TrueCard {
		if r.TrueCard == 0 {
			return (r.EstCard + 1) / (r.TrueCard + 1)
		}
		return r.EstCard / r.TrueCard
	}
	if r.EstCard == 0 {
		return (r.TrueCard + 1) / (r.EstCard + 1)
	}
	return r.TrueCard / r.EstCard
}

/*
	PError is:
		if est > true && true > 0: (est/true) - 1
		if est > true && true == 0: ((est+1)/(true+1)) - 1
		if est <= true && est > 0: 1 - (true/est)
		if est <= true && est == 0: 1 - ((true+1)/(est+1))
*/
func PError(r EstResult) float64 {
	if r.EstCard > r.TrueCard && r.TrueCard > 0 {
		return (r.EstCard / r.TrueCard) - 1
	} else if r.EstCard > r.TrueCard && r.TrueCard == 0 {
		return ((r.EstCard + 1) / (r.TrueCard + 1)) - 1
	} else if r.EstCard <= r.TrueCard && r.EstCard > 0 {
		return 1 - (r.TrueCard / r.EstCard)
	} else {
		return 1 - ((r.TrueCard + 1) / (r.EstCard + 1))
	}
}

// Bias is (est-true)/(true+1).
func Bias(r EstResult) float64 {
	return (r.EstCard - r.TrueCard) / (r.TrueCard + 1)
}

type EstResultCollector interface {
	AddEstResult(insIdx, dsIdx, qtIdx int, r EstResult)
	EstResults(insIdx, dsIdx, qtIdx int) []EstResult
}

func NewEstResultCollector(insCap, dsCap, qtCap int) EstResultCollector {
	rs := make([][][][]EstResult, insCap)
	for i := range rs {
		rs[i] = make([][][]EstResult, dsCap)
		for j := range rs[i] {
			rs[i][j] = make([][]EstResult, qtCap)
		}
	}
	c := new(estResultCollector)
	c.rs = rs
	return c
}

type estResultCollector struct {
	rs   [][][][]EstResult
	lock sync.RWMutex
}

func (c *estResultCollector) AddEstResult(insIdx, dsIdx, qtIdx int, r EstResult) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.rs[insIdx][dsIdx][qtIdx] = append(c.rs[insIdx][dsIdx][qtIdx], r)
}

func (c *estResultCollector) EstResults(insIdx, dsIdx, qtIdx int) []EstResult {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.rs[insIdx][dsIdx][qtIdx]
}

// ExtractEstResult extracts EstResults from results of explain analyze
func ExtractEstResult(analyzeResults [][]string, version string) (EstResult, error) {
	if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v3.0.0") { // v2.x
		return EstResult{}, errors.Errorf("unsupported version=%v", version)
	} else if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v4.0.0") { // v3.x
		return extractEstResultForV3(analyzeResults)
	} else if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v5.0.0") { // v4.x
		return extractEstResultForV4(analyzeResults)
	}
	return EstResult{}, errors.Errorf("unsupported version=%v", version)
}

func extractEstResultForV4(analyzeResults [][]string) (EstResult, error) {
	// | TableReader_5         | 10000.00 | 0       | ...
	est, err := strconv.ParseFloat(analyzeResults[0][1], 64)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	act, err := strconv.ParseFloat(analyzeResults[0][2], 64)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}

	return EstResult{
		EstCard:  est,
		TrueCard: act,
	}, nil
}

func extractEstResultForV3(analyzeResults [][]string) (EstResult, error) {
	//| TableReader_5     | 10000.00 | root | data:TableScan_4                                           | time:2.95024ms, loops:1, rows:0 | 115 Bytes |
	est, err := strconv.ParseFloat(analyzeResults[0][1], 64)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	info := analyzeResults[0][4]
	tmp := strings.Split(info, ":")
	actStr := tmp[len(tmp)-1]
	act, err := strconv.ParseFloat(actStr, 64)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	return EstResult{
		EstCard:  est,
		TrueCard: act,
	}, nil
}
