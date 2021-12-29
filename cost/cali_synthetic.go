package cost

import (
	"fmt"
	"math/rand"

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
	ret = append(ret, genSyntheticCaliScanQueries(ins, n, false)...)
	ret = append(ret, genSyntheticCaliScanQueries(ins, n, true)...)
	return ret
}

func genSyntheticCaliScanQueries(ins tidb.Instance, n int, wide bool) CaliQueries {
	var qs CaliQueries
	var minA, maxA, minB, maxB int
	mustReadOneLine(ins, `select min(a), max(a), min(b), max(b) from t`, &minA, &maxA, &minB, &maxB)

	// PK scan
	step := (maxA - minA) / n
	for i := 0; i < n; i++ {
		la := rand.Intn(step)
		ra := rand.Intn(step) + step*(i+1)
		if ra > maxA {
			ra = maxA
		}
		readCols := "a"
		if wide {
			readCols = "a, c"
		}

		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where a>=%v and a<=%v", la, ra))
		scanW := float64(rowCount) * getSyntheticTableRowSize(readCols, "for-scan")
		netW := float64(rowCount) * getSyntheticTableRowSize(readCols, "for-net")
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, primary) */ %v from t where a>=%v and a<=%v", readCols, la, ra),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	// index scan
	step = (maxB - minB) / n
	for i := 0; i < n; i++ {
		lb := rand.Intn(step)
		rb := rand.Intn(step) + step*(i+1)
		if rb > maxB {
			rb = maxB
		}
		readCols, hint := "b", "b"
		if wide {
			readCols, hint = "b, c", "bc"
		}

		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", lb, rb))
		scanW := float64(rowCount) * getSyntheticIndexRowSize(readCols, "for-scan")
		netW := float64(rowCount) * getSyntheticIndexRowSize(readCols, "for-net")
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, %v) */ %v from t where b>=%v and b<=%v", hint, readCols, lb, rb),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	// index lookup
	step = (maxB - minB) / n
	for i := 0; i < n; i++ {
		lb := rand.Intn(step)
		rb := rand.Intn(step) + step*(i+1)
		if rb > maxB {
			rb = maxB
		}
		readCols := "b, d"
		if wide {
			readCols = "b, c"
		}

		rowCount := mustGetRowCount(ins, fmt.Sprintf("select count(*) from t where b>=%v and b<=%v", lb, rb))
		scanW := float64(rowCount) * (getSyntheticIndexRowSize(readCols, "for-scan") + getSyntheticTableRowSize(readCols, "for-scan"))
		netW := float64(rowCount) * (getSyntheticIndexRowSize(readCols, "for-net") + getSyntheticTableRowSize(readCols, "for-net"))
		qs = append(qs, CaliQuery{
			SQL:          fmt.Sprintf("select /*+ use_index(t, b) */ %v from t where b>=%v and b<=%v", readCols, lb, rb),
			Label:        "",
			FactorVector: [6]float64{0, 0, netW, scanW, 0, 0},
		})
	}

	return qs
}

func genSyntheticCaliDescScanQueries() {

}

func getSyntheticTableRowSize(cols, typ string) float64 {
	//if cols == "a" && typ == "for-scan" {
	//	return 20
	//} else if cols == "a" && typ == "for-net" {
	//	return 8.125
	//} else if cols == "b, d" && typ == "for-scan" {
	//	return 20
	//} else if cols == "b, d" && typ == "for-net" {
	//	return 16.5
	//} else if cols == "a, c" && typ == "for-scan" {
	//	return 20
	//} else if cols == "a, c" && typ == "for-net" {
	//	return 139.23
	//} else if cols == "b, c" && typ == "for-scan" {
	//	return 1
	//} else if cols == "b, c" && typ == "for-net" {
	//	return 1
	//}
	panic(fmt.Sprintf("%v, %v", cols, typ))
}

func getSyntheticIndexRowSize(cols, typ string) float64 {
	//if cols == "b" && typ == "for-scan" {
	//	return 29
	//} else if cols == "b" && typ == "for-net" {
	//	return 8.125
	//} else if cols == "b, c" && typ == "for-scan" {
	//	return 160
	//} else if cols == "b, c" && typ == "for-net" {
	//	return 139.23
	//} else if cols == "b, d" && typ == "for-scan" {
	//	return 38
	//} else if cols == "b, d" && typ == "for-net" {
	//	return 16.5
	//}
	panic(fmt.Sprintf("%v %v", cols, typ))
}
