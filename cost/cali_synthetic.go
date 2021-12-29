package cost

import (
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
)

// Scan
//   select /*+ use_index(t, primary) */ a from t where a>=? and a<=?										(0, 0, estRow*estColSize, estRow*estColSize, 0, 0)
//   select /*+ use_index(t, b) */ b from t where b>=? and b<=?												(0, 0, estRow*estIdxSize, estRow*estIdxSize, 0, 0)
//   select /*+ use_index(t, b) */ b, d from t where b>=? and b<=?											(0, 0, estRow*estColSize+ estRow*estIdxSize, estRow*estColSize+estRow*estIdxSize, 0, 0)
// WideScan
//   select /*+ use_index(t, primary) */ a, c from t where a>=? and a<=?									(0, 0, estRow*estColSize, estRow*estColSize, 0, 0)
//   select /*+ use_index(t, bc) */ b, c from t where b>=? and b<=?											(0, 0, estRow*estIdxSize, estRow*estIdxSize, 0, 0)
//   select /*+ use_index(t, b) */ b, c from t where b>=? and b<=?											(0, 0, estRow*estColSize+ estRow*estIdxSize, estRow*estColSize+estRow*estIdxSize, 0, 0)
// DescScan
//   select /*+ use_index(t, primary), no_reorder() */ a from t where a>=? and a<=? order by a desc			(0, 0, estRow*estColSize, 0, estRow*estColSize, 0)
//   select /*+ use_index(t, b), no_reorder() */ b from t where b>=? and b<=? order by b desc				(0, 0, estRow*estColSize, 0, estRow*estIdxSize, 0)
// Cop-CPU Operations: TODO
// TiDB-CPU Operations: TODO

func genSyntheticCalibrationQueries(ins tidb.Instance, db string) CaliQueries {
	ins.MustExec(fmt.Sprintf(`use %v`, db))
	n := 1
	var ret CaliQueries
	ret = append(ret, genSyntheticCaliScanQueries(ins, n)...)
	return ret
}

var hackRowSize map[string]float64

func init() {
	hackRowSize["Scan-TableScan-scan"] = 0
	hackRowSize["Scan-TableScan-scan"] = 0

}

func getSyntheticRowSize(key string) float64 {
	if _, ok := hackRowSize[key]; !ok {
		panic(key)
	}
	return hackRowSize[key]
}

func genSyntheticCaliScanQueries(ins tidb.Instance, n int) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	for i := 0; i < n; i++ {
		l, r := randRange(minA, maxA, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-TableScan-scan")
		netW := float64(rowCount) * getSyntheticRowSize("Scan-TableScan-net")
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, primary) */ a from t where a>=%v and a<=%v", l, r),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-IndexScan-scan")
		netW := float64(rowCount) * getSyntheticRowSize("Scan-IndexScan-net")
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, b) */ b from t where b>=%v and b<=%v", l, r),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	for i := 0; i < n; i++ {
		l, r := randRange(minB, maxB, i, n)
		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", l, r))
		scanW := float64(rowCount) * getSyntheticRowSize("Scan-IndexLookup-scan")
		netW := float64(rowCount) * getSyntheticRowSize("Scan-IndexLookup-net")
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, b) */ b, d from t where b>=%v and b<=%v", l, r),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliDescScanQueries() {

}
