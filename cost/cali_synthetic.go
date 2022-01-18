package cost

import (
	"fmt"
	"math"

	"github.com/qw4990/OptimizerTester/tidb"
)

// Scan: scanFactor, netFactor																				(CPU, CopCPU, Net, Scan, DescScan, Mem)
//   select /*+ use_index(t, primary) */ a from t where a>=? and a<=?										(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b from t where b>=? and b<=?												(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b, d from t where b>=? and b<=?											(estRow*(1+ log2(Min(estRow, lookupBatchSize))), 0, estRow*log(tblRowSize)+estRow*log(idxRowSize), estRow*tblRowSize+estRow*idxRowSize, 0, 0)
// WideScan: scanFactor, netFactor
//   select /*+ use_index(t, primary) */ a, c from t where a>=? and a<=?									(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, bc) */ b, c from t where b>=? and b<=?											(0, 0, estRow*log(rowSize), estRow*rowSize, 0, 0)
//   select /*+ use_index(t, b) */ b, c from t where b>=? and b<=?											(estRow*(1+ log2(Min(estRow, lookupBatchSize))), 0, estRow*log(tblRowSize)+estRow*log(idxRowSize), estRow*tblRowSize+estRow*idxRowSize, 0, 0)
// DescScan: descScanFactor, netFactor
//   select /*+ use_index(t, primary), no_reorder() */ a from t where a>=? and a<=? order by a desc			(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
//   select /*+ use_index(t, b), no_reorder() */ b from t where b>=? and b<=? order by b desc				(0, 0, estRow*rowSize, 0, estRow*log(rowSize), 0)
// AGG: CPUFactor, copCPUFactor
//   select /*+ use_index(t, b), stream_agg(), agg_to_cop() */ count(1) from t where b>=? and b<=?			(0, estRow, 0, estRow*log(rowSize), 0, 0)
//   select /*+ use_index(t, b), stream_agg(), agg_not_to_cop */ count(1) from t where b>=? and b<=?		(estRow, 0, estRow*rowSize, estRow*log(rowSize), 0, 0)
// Sort: CPUFactor, MemFactor
//   select /*+ use_index(t, b), must_reorder() */ b from t where b>=? and b<=? order by b					(estRow*log(estRow), 0, estRow*rowSize, estRow*log(rowSize), 0, estRow)

func genSyntheticCalibrationQueries(ins tidb.Instance, db string) CaliQueries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	n := 5
	var ret CaliQueries
	ret = append(ret, genSyntheticCaliScanQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliWideScanQueries(ins, n)...)
	ret = append(ret, genSyntheticCaliDescScanQueries(ins, n)...)
	return ret
}

var hackRowSize map[string]float64

func init() {
	hackRowSize = make(map[string]float64)
	hackRowSize["Scan-TableScan-scan"] = 4.321928094887363
	hackRowSize["Scan-TableScan-net"] = 8.125
	hackRowSize["Scan-IndexScan-scan"] = 4.857980995127573
	hackRowSize["Scan-IndexScan-net"] = 8.125
	hackRowSize["Scan-IndexLookup-scan"] = 12.723660944409982
	hackRowSize["Scan-IndexLookup-net"] = 16.25 + 16.25

	hackRowSize["WideScan-TableScan-scan"] = 7.311339625955218
	hackRowSize["WideScan-TableScan-net"] = 139.23
	hackRowSize["WideScan-IndexScan-scan"] = 7.321928094887362
	hackRowSize["WideScan-IndexScan-net"] = 139.23
	hackRowSize["WideScan-IndexLookup-scan"] = 12.723660944409982
	hackRowSize["WideScan-IndexLookup-net"] = 16.25 + 139.23
}

func getSyntheticRowSize(key string, costVariant int) float64 {
	if _, ok := hackRowSize[key]; !ok {
		panic(key)
	}
	rowSize := hackRowSize[key]
	return rowSize
}

func genSyntheticCaliWideScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("WideScan-TableScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("WideScan-TableScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary) */ a, c from t where a>=%v and a<=%v", l, r),
			Label:   "Wide-TableScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("WideScan-IndexScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("WideScan-IndexScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, bc) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:   "Wide-IndexScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("WideScan-IndexLookup-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("WideScan-IndexLookup-net", 1)
		cpuW := float64(rowCount) * (1.0 + math.Log2(math.Min(float64(rowCount), float64(20000))))
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b, c from t where b>=%v and b<=%v", l, r),
			Label:   "Wide-IndexLookup",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-TableScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("Scan-TableScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary) */ a from t where a>=%v and a<=%v", l, r),
			Label:   "TableScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-IndexScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("Scan-IndexScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b from t where b>=%v and b<=%v", l, r),
			Label:   "IndexScan",
			Weights: CostWeights{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-IndexLookup-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("Scan-IndexLookup-net", 1)
		cpuW := float64(rowCount) * (1.0 + math.Log2(math.Min(float64(rowCount), float64(20000))))
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b) */ b, d from t where b>=%v and b<=%v", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{cpuW, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliDescScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// table scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and b<=%v", l, r))
		descScanW := float64(rowCount) * getSyntheticRowSize("DescScan-TableScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("DescScan-TableScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, primary), no_reorder() */ a from t where a>=%v and a<=%v order by a desc", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{0, 0, netW, 0, descScanW, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and b<=%v", l, r))
		descScanW := float64(rowCount) * getSyntheticRowSize("DescScan-IndexScan-scan", 1)
		netW := float64(rowCount) * getSyntheticRowSize("DescScan-IndexScan-net", 1)
		qs = append(qs, CaliQuery{
			SQL:     fmt.Sprintf("select /*+ use_index(t, b), no_reorder() */ b from t where b>=%v and b<=%v order by b desc", l, r),
			Label:   "IndexLookup",
			Weights: CostWeights{0, 0, netW, 0, descScanW, 0},
		})
	}
	return qs
}
