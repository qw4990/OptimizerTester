package cetest

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/qw4990/OptimizerTester/tidb"
)

func getEstRowFromExplain(ins tidb.Instance, query string) (estRow float64, re error) {
	sql := "EXPLAIN " + query
	rows, err := ins.Query(sql)
	if err != nil {
		return 0, fmt.Errorf("run sql=%v, err=%v", sql, err)
	}
	defer func() {
		if err := rows.Close(); err != nil && re == nil {
			re = err
		}
	}()

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	nCols := len(types)
	results := make([][]string, 0, 8)
	for rows.Next() {
		cols := make([]string, nCols)
		ptrs := make([]interface{}, nCols)
		for i := 0; i < nCols; i++ {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
		results = append(results, cols)
	}

	return ExtractEstRows(results, ins.Version())
}

func ExtractEstRows(explainResults [][]string, version string) (float64, error) {
	if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v3.0.0") { // v2.x
		panic("TODO")
	} else if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v4.0.0") { // v3.x
		panic("TODO")
	} else if tidb.ToComparableVersion(version) < tidb.ToComparableVersion("v5.0.0") { // v4.x
		return extractEstRowsForV4(explainResults)
	}
	return 0, nil
}

func extractEstRowsForV4(explainResults [][]string) (float64, error) {
	// | IndexReader_6          | 0.00    | root      |                             | index:IndexRangeScan_5 
	est, err := strconv.ParseFloat(explainResults[0][1], 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return est, nil
}

func getEstResultFromExplainAnalyze(ins tidb.Instance, query string) (r EstResult, re error) {
	begin := time.Now()
	sql := "EXPLAIN ANALYZE " + query
	rows, err := ins.Query(sql)
	if err != nil {
		return EstResult{}, errors.Trace(err)
	}
	if time.Since(begin) > time.Millisecond*50 {
		fmt.Printf("[SLOW QUERY] %v cost %v\n", sql, time.Since(begin))
	}
	defer func() {
		if err := rows.Close(); err != nil && re == nil {
			re = err
		}
	}()

	types, err := rows.ColumnTypes()
	if err != nil {
		return EstResult{}, err
	}
	nCols := len(types)
	results := make([][]string, 0, 8)
	for rows.Next() {
		cols := make([]string, nCols)
		ptrs := make([]interface{}, nCols)
		for i := 0; i < nCols; i++ {
			ptrs[i] = &cols[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return EstResult{}, err
		}
		results = append(results, cols)
	}

	return ExtractEstResult(results, ins.Version())
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
